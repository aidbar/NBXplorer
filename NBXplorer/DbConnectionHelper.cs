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

		public record NewOut(uint256 txId, int idx, Script script, Money value, string? assetId = null);
		public record NewIn(uint256 txId, int idx, uint256 spentTxId, int spentIdx);

		public async Task FetchMatches(IEnumerable<NewOut>? newOuts, IEnumerable<NewIn>? newIns)
		{
			newOuts ??= Array.Empty<NewOut>();
			newIns ??= Array.Empty<NewIn>();
			int i = 0;
			StringBuilder cmd = new StringBuilder();
			cmd.Append("CALL fetch_matches(@code, ");
			cmd.Append("ARRAY[");
			foreach (var o in newOuts)
			{
				if (i != 0)
					cmd.Append(',');
				var asset_id = o.assetId ?? String.Empty;
				cmd.Append($"('{o.txId}', {o.idx}, '{o.script.ToHex()}', {o.value.Satoshi}, '{asset_id}')");
				i++;
			}
			cmd.Append("]::new_out[],");
			i = 0;
			cmd.Append("ARRAY[");
			foreach (var ni in newIns)
			{
				if (i != 0)
					cmd.Append(',');
				cmd.Append($"('{ni.txId}', {ni.idx}, '{ni.spentTxId}', {ni.spentIdx})");
				i++;
			}
			cmd.Append("]::new_in[])");
			await Connection.ExecuteAsync(cmd.ToString(), new { code = Network.CryptoCode });
		}
		public async Task SaveTransactions(IEnumerable<(Transaction? Transaction, uint256? Id, uint256? BlockId, int? BlockIndex, long? BlockHeight, bool immature)> transactions, DateTimeOffset? now)
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
				blk_idx = tx.BlockIndex is int i ? i : 0,
				blk_height = tx.BlockHeight,
				immature = tx.immature
			})
			.Where(o => o.id is not null)
			.ToArray();
			if (now is null)
				await Connection.ExecuteAsync("INSERT INTO txs VALUES (@code, @id, @raw, @immature) ON CONFLICT (code, tx_id) DO UPDATE SET raw = COALESCE(@raw, txs.raw)", parameters);
			else
				await Connection.ExecuteAsync("INSERT INTO txs VALUES (@code, @id, @raw, @immature, @blk_id, @blk_idx, @blk_height, @mempool, NULL, @seen_at) ON CONFLICT (code, tx_id) DO UPDATE SET seen_at=LEAST(@seen_at, txs.seen_at), raw = COALESCE(@raw, txs.raw)", parameters);
			await Connection.ExecuteAsync("INSERT INTO blks_txs VALUES (@code, @blk_id, @id, @blk_idx) ON CONFLICT DO NOTHING", parameters.Where(p => p.blk_id is not null).AsList());
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
