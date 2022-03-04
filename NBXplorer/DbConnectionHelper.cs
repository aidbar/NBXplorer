#nullable enable
using Dapper;
using Microsoft.Extensions.Logging;
using NBitcoin;
using NBitcoin.DataEncoders;
using NBXplorer.Configuration;
using NBXplorer.DerivationStrategy;
using NBXplorer.Logging;
using NBXplorer.Models;
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

		public async Task SaveTransactions(IEnumerable<(Transaction? Transaction, uint256? Id, uint256? BlockId, int? BlockIndex)> transactions, DateTimeOffset? now)
		{
			var parameters = transactions.Select(tx =>
			new {
				code = Network.CryptoCode,
				blk_id = tx.BlockId?.ToString(),
				id = tx.Id?.ToString() ?? tx.Transaction?.GetHash()?.ToString(),
				raw = tx.Transaction?.ToBytes(),
				seen_at = now is null ? default : now.Value,
				blk_idx = tx.BlockIndex is int i ? i : 0
			})
			.Where(o => o.id is not null)
			.ToArray();
			if (now is null)
				await Connection.ExecuteAsync("INSERT INTO txs VALUES (@code, @id, @raw) ON CONFLICT (code, tx_id) DO UPDATE SET raw = COALESCE(@raw, txs.raw)", parameters);
			else
				await Connection.ExecuteAsync("INSERT INTO txs VALUES (@code, @id, @raw, NULL, NULL, @seen_at) ON CONFLICT (code, tx_id) DO UPDATE SET seen_at=LEAST(@seen_at, txs.seen_at), raw = COALESCE(@raw, txs.raw)", parameters);
			await Connection.ExecuteAsync("INSERT INTO txs_blks VALUES (@code, @id, @blk_id, @blk_idx) ON CONFLICT DO NOTHING", parameters.Where(p => p.blk_id is not null).AsList());
		}
		public async Task CreateDescriptors(string walletId, Descriptor[] descriptors)
		{
			var rows = descriptors.Select(c => new { code = Network.CryptoCode, descriptor = c.ToString(), walletId }).ToArray();
			await Connection.ExecuteAsync(
				"INSERT INTO descriptors VALUES (@code, @descriptor) ON CONFLICT DO NOTHING;" +
				"INSERT INTO descriptors_wallets VALUES (@code, @descriptor, @walletId) ON CONFLICT DO NOTHING", rows);
		}

		public async Task<List<UTXO>> GetWalletUTXOWithDescriptors(NBXplorerNetwork network, string walletId, int currentHeight)
		{
			var rows = await Connection.QueryAsync<(string tx_id, int idx, string script, long value, bool immature, string keypath, long height, DateTime seen_at)>
							("SELECT tx_id, u.idx, script, value, immature, keypath, height, t.seen_at FROM get_wallet_conf_utxos(@code, @walletId) u " +
							"INNER JOIN tracked_scripts ts USING (code, script) " +
							"INNER JOIN txs t USING (code, tx_id) " +
							$"WHERE u.code=@code AND ts.wallet_id=@walletId", new { code = network.CryptoCode, walletId });
			rows.TryGetNonEnumeratedCount(out var c);
			var result = new List<UTXO>(c);
			foreach (var row in rows)
			{
				if (row.immature)
					continue;
				var txid = uint256.Parse(row.tx_id);
				var keypath = row.keypath is null ? null : KeyPath.Parse(row.keypath);
				var utxo = new UTXO()
				{
					Confirmations = (int)(currentHeight - row.height + 1),
					Index = row.idx,
					Outpoint = new OutPoint(txid, row.idx),
					KeyPath = keypath,
					ScriptPubKey = Script.FromHex(row.script),
					Timestamp = new DateTimeOffset(row.seen_at),
					TransactionHash = txid,
					Value = Money.Satoshis(row.value),
					Feature = keypath is null ? null : KeyPathTemplates.GetDerivationFeature(keypath)
				};
				result.Add(utxo);
			}
			return result;
		}

		record DescriptorScriptInsert(string code, string descriptor, int idx, string script, string keypath, string addr);
		public async Task<int> GenerateAddresses(Descriptor descriptor, GenerateAddressQuery? query)
		{
			query = query ?? new GenerateAddressQuery();
			var used = await Connection.QueryAsync<bool>(
				"SELECT s.used FROM descriptors_scripts ds " +
				"INNER JOIN scripts s USING (code, script) " +
				"WHERE ds.code=@code AND ds.descriptor=@descriptor " +
				"ORDER BY ds.idx DESC " +
				"LIMIT @limit", new { code = Network.CryptoCode, descriptor = descriptor.ToString(), limit = MinPoolSize });
			var unused = used.TakeWhile(u => !u).Count();
			if (unused >= MinPoolSize)
				return 0;
			var toGenerate = Math.Max(0, MaxPoolSize - unused);
			if (query.MaxAddresses is int max)
				toGenerate = Math.Min(max, toGenerate);
			if (query.MinAddresses is int min)
				toGenerate = Math.Max(min, toGenerate);
			if (toGenerate == 0)
				return 0;
			retry:
			var row = await Connection.ExecuteScalarAsync<int?>("SELECT next_index FROM descriptors WHERE code=@code AND descriptor=@descriptor", new { code = Network.CryptoCode, descriptor = descriptor.ToString() });
			if (row is null && descriptor is LegacyDescriptor legacy)
			{
				var wid = new DerivationSchemeTrackedSource(legacy.DerivationStrategy).GetLegacyWalletId(Network);
				await this.CreateWallet(wid);
				await this.CreateDescriptors(wid, new[] { descriptor });
				goto retry;
			}
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

			await Connection.ExecuteAsync(
				"INSERT INTO scripts VALUES (@code, @script, @addr) ON CONFLICT DO NOTHING;" +
				"INSERT INTO descriptors_scripts VALUES (@code, @descriptor, @idx, @script, @keypath) ON CONFLICT DO NOTHING;", linesScriptpubkeys);
			await Connection.ExecuteAsync("UPDATE descriptors SET next_index=@next_index WHERE code=@code AND descriptor=@descriptor AND next_index < @next_index;", new { code = Network.CryptoCode, descriptor = descStr, next_index = nextIndex + toGenerate });
			return toGenerate;
		}

		record SpentUTXORow(System.String code, System.String tx_id, System.Int32 idx, System.String script, System.Int64 value, bool immature, System.DateTime created_at, string spent_outpoint);
		public Task<Dictionary<OutPoint, TxOut>> GetUTXOs(IList<OutPoint> outPoints)
		{
			return GetUTXOs(outPoints.Select(o => o.ToString()).ToArray());
		}

		record UnusedScriptRow(string script, string addr, string keypath);
		public async Task<KeyPathInformation?> GetUnused(LegacyDescriptor descriptor, int skip, bool reserve)
		{
			retry:
			var unused = await Connection.QueryFirstOrDefaultAsync<UnusedScriptRow>(
				"SELECT s.script, s.addr, ds.keypath FROM descriptors_scripts ds " +
				"INNER JOIN scripts s USING (code, script) " +
				"WHERE ds.code=@code AND ds.descriptor=@descriptor AND s.used='f' " +
				"LIMIT 1 OFFSET @skip", new { code = Network.CryptoCode, descriptor = descriptor.ToString(), skip });
			if (unused is null)
				return null;
			if (reserve)
			{
				var updated = await Connection.ExecuteAsync("UPDATE scripts SET used='t' WHERE code=@code AND script=@script AND used='f'", new { code = Network.CryptoCode, unused.script });
				if (updated == 0)
					goto retry;
			}
			var keypath = KeyPath.Parse(unused.keypath);
			return new KeyPathInformation()
			{
				Address = BitcoinAddress.Create(unused.addr, Network.NBitcoinNetwork),
				DerivationStrategy = descriptor.DerivationStrategy,
				KeyPath = keypath,
				ScriptPubKey = Script.FromHex(unused.script),
				TrackedSource = new DerivationSchemeTrackedSource(descriptor.DerivationStrategy),
				Feature = KeyPathTemplates.GetDerivationFeature(keypath),
				// TODO: Redeem = 
			};
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

		public async Task SetMetadata<TMetadata>(string walletId, string key, TMetadata value) where TMetadata : class
		{
			if (value is null)
				await Connection.ExecuteAsync("DELETE FROM wallet_metadata WHERE wallet_id=@walletId AND key=@key", new { walletId, key });
			else
				await Connection.ExecuteAsync("INSERT INTO wallet_metadata VALUES (@walletId, @key, @data::JSONB) ON CONFLICT (wallet_id, key) DO UPDATE SET data=@data::JSONB", new { walletId, key, data = Network.Serializer.ToString(value) });
		}
		public async Task<TMetadata?> GetMetadata<TMetadata>(string walletId, string key) where TMetadata : class
		{
			var result = await Connection.ExecuteScalarAsync<string?>("SELECT data FROM wallet_metadata WHERE wallet_id=@walletId AND key=@key", new { walletId, key });
			if (result is null)
				return null;
			return Network.Serializer.ToObject<TMetadata>(result);
		}
	}
}
