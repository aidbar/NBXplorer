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
using System.Text;
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

		public record NewOut(uint256 txId, int idx, Script script, Money value, string? assetId = null);
		public record NewIn(uint256 txId, int idx, uint256 spentTxId, int spentIdx);

		public async Task NewTxs(NewOut[]? newOuts, NewIn[]? newIns, DateTimeOffset seenAt)
		{
			newOuts ??= Array.Empty<NewOut>();
			newIns ??= Array.Empty<NewIn>();
			int i = 0;
			StringBuilder tx_outs = new StringBuilder();
			tx_outs.Append("ARRAY[");
			foreach (var o in newOuts)
			{
				if (i != 0)
					tx_outs.Append(',');
				var asset_id = o.assetId is null ? "''" : o.assetId;
				tx_outs.Append($"('{o.txId}', {o.idx}, {o.script.ToHex()}, {o.value.Satoshi}, {asset_id})");
			}
			tx_outs.Append("]::new_out[]");
			i = 0;
			StringBuilder tx_ins = new StringBuilder();
			tx_ins.Append("ARRAY[");
			foreach (var ni in newIns)
			{
				if (i != 0)
					tx_ins.Append(',');
				tx_ins.Append($"('{ni.txId}', {ni.idx}, '{ni.spentTxId}', {ni.spentIdx})");
				i++;
			}
			tx_ins.Append("]::new_in[]");
			await Connection.ExecuteAsync($"CALL new_txs(@code, {tx_outs}, {tx_ins}, @seen_at)", new { code = Network.CryptoCode, seen_at = seenAt.UtcDateTime });
		}

		public async Task InsertIns(IEnumerable<(uint256 inputTxId, int inputIdx, OutPoint spentOutpoint)> ins)
		{
			var dbCommand = Connection.CreateCommand();
			int idx = 0;
			StringBuilder builder = new StringBuilder();
			builder.Append("INSERT INTO ins VALUES ");
			foreach (var i in ins)
			{
				if (idx != 0)
					builder.Append(',');
				// No injection possible, those are strongly typed
				builder.Append($"('{Network.CryptoCode}', '{i.inputTxId}', {i.inputIdx}, '{i.spentOutpoint.Hash}', {i.spentOutpoint.N})");
				idx++;
			}
			if (idx == 0)
				return;
			builder.Append(" ON CONFLICT DO NOTHING;");
			dbCommand.CommandText = builder.ToString();
			await dbCommand.ExecuteNonQueryAsync();
		}

		public async Task InsertOuts(IEnumerable<(OutPoint outpoint, TxOut output, bool immature)> outs)
		{
			var dbCommand = Connection.CreateCommand();
			int idx = 0;
			StringBuilder builder = new StringBuilder();
			builder.Append("INSERT INTO outs (code, tx_id, idx, script, value, immature) VALUES ");
			foreach (var o in outs)
			{
				if (idx != 0)
					builder.Append(',');
				var immatureBool = o.immature ? "'t'" : "'f'";
				builder.Append($"('{Network.CryptoCode}', '{o.outpoint.Hash}', {o.outpoint.N}, '{o.output.ScriptPubKey.ToHex()}', {o.output.Value.Satoshi}, {immatureBool})");
				idx++;
			}
			if (idx == 0)
				return;
			builder.Append(" ON CONFLICT DO NOTHING;");
			dbCommand.CommandText = builder.ToString();
			await dbCommand.ExecuteNonQueryAsync();
		}
		public async Task SaveTransactions(IEnumerable<(Transaction? Transaction, uint256? Id, uint256? BlockId, int? BlockIndex)> transactions, DateTimeOffset? now)
		{
			var parameters = transactions.Select(tx =>
			new
			{
				code = Network.CryptoCode,
				blk_id = tx.BlockId?.ToString(),
				id = tx.Id?.ToString() ?? tx.Transaction?.GetHash()?.ToString(),
				raw = tx.Transaction?.ToBytes(),
				mempool = tx.BlockId is null,
				seen_at = now is null ? default : now.Value,
				blk_idx = tx.BlockIndex is int i ? i : 0
			})
			.Where(o => o.id is not null)
			.ToArray();
			if (now is null)
				await Connection.ExecuteAsync("INSERT INTO txs VALUES (@code, @id, @raw) ON CONFLICT (code, tx_id) DO UPDATE SET raw = COALESCE(@raw, txs.raw)", parameters);
			else
				await Connection.ExecuteAsync("INSERT INTO txs VALUES (@code, @id, @raw, @blk_id, @blk_idx, @mempool, NULL, @seen_at) ON CONFLICT (code, tx_id) DO UPDATE SET seen_at=LEAST(@seen_at, txs.seen_at), raw = COALESCE(@raw, txs.raw)", parameters);
			await Connection.ExecuteAsync("INSERT INTO blks_txs VALUES (@code, @blk_id, @id, @blk_idx) ON CONFLICT DO NOTHING", parameters.Where(p => p.blk_id is not null).AsList());
		}
		public async Task CreateDescriptors(Descriptor[] descriptors)
		{
			var rows = descriptors.Select(c => new { code = Network.CryptoCode, descriptor = c.ToString() }).ToArray();
			await Connection.ExecuteAsync(
				"INSERT INTO descriptors VALUES (@code, @descriptor) ON CONFLICT DO NOTHING;", rows);
		}
		record DescriptorScriptInsert(string code, string descriptor, int idx, string script, string keypath, string addr, string walletid);
		public async Task<int> GenerateAddresses(string walletId, Descriptor descriptor, GenerateAddressQuery? query)
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
				await this.CreateDescriptors(new[] { descriptor });
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
					derivation.ScriptPubKey.GetDestinationAddress(Network.NBitcoinNetwork).ToString(),
					walletId);
			});

			await Connection.ExecuteAsync(
				"INSERT INTO scripts VALUES (@code, @script, @addr) ON CONFLICT DO NOTHING;" +
				"INSERT INTO descriptors_scripts VALUES (@code, @descriptor, @idx, @script, @keypath) ON CONFLICT DO NOTHING;" +
				"INSERT INTO wallets_scripts VALUES(@code, @script, @walletid, @descriptor, @idx) ON CONFLICT DO NOTHING;", linesScriptpubkeys);
			await Connection.ExecuteAsync("UPDATE descriptors SET next_index=@next_index WHERE code=@code AND descriptor=@descriptor AND next_index < @next_index;", new { code = Network.CryptoCode, descriptor = descStr, next_index = nextIndex + toGenerate });
			return toGenerate;
		}

		public async Task<Dictionary<OutPoint, TxOut>> GetOutputs(IEnumerable<OutPoint> outPoints)
		{
			Dictionary<OutPoint, TxOut> result = new Dictionary<OutPoint, TxOut>();
			var command = Connection.CreateCommand();
			var builder = new StringBuilder();
			builder.Append("SELECT o.tx_id, o.idx, o.value, o.script FROM (VALUES ");
			int idx = 0;
			foreach (var o in outPoints)
			{
				if (idx != 0)
					builder.Append(',');
				builder.Append($"('{Network.CryptoCode}', '{o.Hash}', {o.N})");
				idx++;
			}
			if (idx == 0)
				return result;
			builder.Append(") r (code, tx_id, idx) JOIN outs o USING (code, tx_id, idx);");
			command.CommandText = builder.ToString();
			using var reader = await command.ExecuteReaderAsync();
			while (await reader.ReadAsync())
			{
				var txout = this.Network.NBitcoinNetwork.Consensus.ConsensusFactory.CreateTxOut();
				txout.Value = Money.Satoshis(reader.GetInt64(2));
				txout.ScriptPubKey = Script.FromHex(reader.GetString(3));
				result.TryAdd(new OutPoint(uint256.Parse(reader.GetString(0)), reader.GetInt32(1)), txout);
			}
			return result;
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

		public async Task<SlimChainedBlock?> GetTip()
		{
			var row = await Connection.QueryFirstOrDefaultAsync("SELECT * FROM get_tip(@code);", new { code = Network.CryptoCode });
			if (row is null)
				return null;
			return new SlimChainedBlock(uint256.Parse(row.blk_id), uint256.Parse(row.prev_id), (int)row.height);
		}

		public async Task<bool> SetMetadata<TMetadata>(string walletId, string key, TMetadata value) where TMetadata : class
		{
			if (value is null)
				return await Connection.ExecuteAsync("UPDATE wallets w SET metadata=(w.metadata - @key) WHERE wallet_id=@walletId", new { walletId, key }) == 1;
			else
				return await Connection.ExecuteAsync("UPDATE wallets w SET metadata=jsonb_set(COALESCE(w.metadata,'{}'), array[@key], @data::jsonb) WHERE wallet_id=@walletId", new { walletId, key, data = Network.Serializer.ToString(value) }) == 1;
		}
		public async Task<TMetadata?> GetMetadata<TMetadata>(string walletId, string key) where TMetadata : class
		{
			var result = await Connection.ExecuteScalarAsync<string?>("SELECT metadata->@key FROM wallets WHERE wallet_id=@walletId", new { walletId, key });
			if (result is null)
				return null;
			return Network.Serializer.ToObject<TMetadata>(result);
		}
	}
}
