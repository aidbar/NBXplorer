#nullable enable
using Dapper;
using NBitcoin;
using NBitcoin.DataEncoders;
using NBXplorer.Configuration;
using NBXplorer.DerivationStrategy;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace NBXplorer
{
	public class DbConnectionHelper : IDisposable, IAsyncDisposable
	{
		public DbConnectionHelper(NBXplorerNetwork network,
									DbConnection connection,
									KeyPathTemplates keyPathTemplates)
		{
			derivationStrategyFactory = new DerivationStrategyFactory(network.NBitcoinNetwork);
			Network = network;
			Connection = connection;
			KeyPathTemplates = keyPathTemplates;
		}
		DerivationStrategyFactory derivationStrategyFactory;

		public NBXplorerNetwork Network { get; }
		public DbConnection Connection { get; }
		public KeyPathTemplates KeyPathTemplates { get; }
		public int MinPoolSize { get; set; }
		public int MaxPoolSize { get; set; }

		public void Dispose()
		{
			Connection.Dispose();
		}

		public ValueTask DisposeAsync()
		{
			return Connection.DisposeAsync();
		}

		public async Task<bool> CreateWallet(string walletId)
		{
			return await Connection.ExecuteAsync("INSERT INTO wallets VALUES (@id) ON CONFLICT DO NOTHING", new { id = walletId }) == 1;
		}

		public async Task SaveTransactions(Transaction[] transactions, uint256 blockId, DateTimeOffset? now)
		{
			var txs = transactions.Select(tx =>
			new {
				code = Network.CryptoCode,
				id = tx.GetHash().ToString(),
				raw = tx.ToBytes(),
				mempool = blockId is null,
				indexed_at = now is null ? default : now.Value,
			}).ToArray();
			if (now is null)
				await Connection.ExecuteAsync("INSERT INTO txs VALUES (@code, @id, @raw, @mempool) ON CONFLICT (code, tx_id) DO UPDATE SET raw = COALESCE(@raw, txs.raw), mempool = @mempool", txs);
			else
				await Connection.ExecuteAsync("INSERT INTO txs VALUES (@code, @id, @raw, @mempool, @indexed_at) ON CONFLICT (code, tx_id) DO UPDATE SET indexed_at=LEAST(@indexed_at, txs.indexed_at), raw = COALESCE(@raw, txs.raw), mempool = @mempool", txs);
			if (blockId is not null)
			{
				var txs_blks = transactions.Select(tx =>
				new {
					code = Network.CryptoCode,
					id = tx.GetHash().ToString(),
					blk_id = blockId.ToString()
				});
				await Connection.ExecuteAsync("INSERT INTO txs_blks VALUES (@code, @id, @blk_id) IF CONFLICT DO NOTHING", txs_blks);
			}
		}
		public async Task CreateDescriptors(string walletId, Descriptor[] descriptors)
		{
			var rows = descriptors.Select(c => new { code = Network.CryptoCode, descriptor = c.ToString(), walletId }).ToArray();
			await Connection.ExecuteAsync("INSERT INTO descriptors VALUES (@code, @descriptor) ON CONFLICT DO NOTHING", rows);
			await Connection.ExecuteAsync("INSERT INTO descriptors_wallets VALUES (@code, @descriptor, @walletId) ON CONFLICT DO NOTHING", rows);
		}

		record DescriptorScriptInsert(string code, string descriptor, int idx, string script, string keypath, string addr);
		public async Task<int> GenerateAddresses(Descriptor descriptor, GenerateAddressQuery? query)
		{
			query = query ?? new GenerateAddressQuery();
			var useds = await Connection.QueryAsync<bool>(
				"SELECT s.used FROM descriptors_scripts ds " +
				"INNER JOIN scripts s USING (code, script) " +
				"WHERE ds.code=@code AND ds.descriptor=@descriptor " +
				"LIMIT @limit", new { code = Network.CryptoCode, descriptor = descriptor.ToString(), limit = MinPoolSize });
			var unused = useds.TakeWhile(u => !u).Count();
			if (unused >= MinPoolSize)
				return 0;
			var toGenerate = Math.Max(0, MaxPoolSize - unused);
			if (query.MaxAddresses is int max)
				toGenerate = Math.Min(max, toGenerate);
			if (query.MinAddresses is int min)
				toGenerate = Math.Max(min, toGenerate);
			if (toGenerate == 0)
				return 0;

			var row = await Connection.ExecuteScalarAsync<int?>("SELECT next_index FROM descriptors WHERE code=@code AND descriptor=@descriptor", new { code = Network.CryptoCode, descriptor = descriptor.ToString() });
			if (row is null)
				return 0;
			var nextIndex = row.Value;

			var line = descriptor.GetLineFor();
			var scriptpubkeys = new Script[toGenerate];
			var linesScriptpubkeys = new DescriptorScriptInsert[toGenerate];
			var descStr = descriptor.ToString()!;
			Parallel.For(nextIndex, nextIndex + toGenerate, i =>
			{
				var derivation = line.Derive((uint)i);
				scriptpubkeys[i - nextIndex] = derivation.ScriptPubKey;
				linesScriptpubkeys[i - nextIndex] = new DescriptorScriptInsert(
					Network.CryptoCode,
					descStr,
					i,
					derivation.ScriptPubKey.ToHex(),
					line.KeyPathTemplate.GetKeyPath(i, false).ToString(),
					derivation.ScriptPubKey.GetDestinationAddress(Network.NBitcoinNetwork).ToString());
			});
			await Connection.ExecuteAsync("INSERT INTO scripts VALUES (@code, @script, @addr) ON CONFLICT DO NOTHING", linesScriptpubkeys);
			return await Connection.ExecuteAsync("INSERT INTO descriptors_scripts VALUES (@code, @descriptor, @idx, @script, @keypath) ON CONFLICT DO NOTHING", linesScriptpubkeys);
		}

		record SpentUTXORow(System.String code, System.String tx_id, System.Int32 idx, System.String script, System.Int64 value, System.DateTime created_at, string spent_outpoint);
		public Task<Dictionary<OutPoint, TxOut>> GetUTXOs(IList<OutPoint> outPoints)
		{
			return GetUTXOs(outPoints.Select(o => o.ToString()).ToArray());
		}
		public async Task<Dictionary<OutPoint, TxOut>> GetUTXOs(IList<string> outPoints)
		{
			Dictionary<OutPoint, TxOut> result = new Dictionary<OutPoint, TxOut>();
			foreach (var row in await Connection.QueryAsync<SpentUTXORow>(
"SELECT *, to_outpoint(tx_id, idx) spent_outpoint FROM outs " +
"WHERE code = @code AND to_outpoint(tx_id, idx) = ANY(@outputQuery)", new { code = Network.CryptoCode, outputQuery = outPoints }))
			{
				var txout = this.Network.NBitcoinNetwork.Consensus.ConsensusFactory.CreateTxOut();
				txout.Value = Money.Satoshis(row.value);
				txout.ScriptPubKey = Script.FromBytesUnsafe(Encoders.Hex.DecodeData(row.script));
				result.TryAdd(new OutPoint(uint256.Parse(row.tx_id), row.idx), txout);
			}
			return result;
		}

		record BlockRow(System.String blk_id, System.String prev_id, System.Int64 height);
		public async Task<SlimChainedBlock?> GetTip()
		{
			var row = await Connection.QueryFirstOrDefaultAsync<BlockRow>("SELECT blk_id, prev_id, height FROM blks WHERE code=@code AND confirmed = 't' LIMIT 1", new { code = Network.CryptoCode });
			if (row is null)
				return null;
			return new SlimChainedBlock(uint256.Parse(row.blk_id), uint256.Parse(row.prev_id), (int)row.height);
		}
	}
}
