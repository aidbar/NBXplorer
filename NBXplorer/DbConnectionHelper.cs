#nullable enable
using Dapper;
using NBitcoin;
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

		record class ScriptPubKeyInsert(string code, string walletid, string scriptPubKey, string addr);
		public async Task AddScriptPubKeys(string walletId, params Script[] scriptPubKeys)
		{
			var rows = scriptPubKeys
				.Select(s => new ScriptPubKeyInsert(Network.CryptoCode, walletId, s.ToHex(), s.GetDestinationAddress(Network.NBitcoinNetwork).ToString()))
				.ToList();
			await Connection.ExecuteAsync("INSERT INTO scriptpubkeys VALUES (@code, @scriptPubKey, @addr) ON CONFLICT DO NOTHING", rows);
			await Connection.ExecuteAsync("INSERT INTO scriptpubkeys_wallets VALUES (@code, @scriptPubKey, @walletid) ON CONFLICT DO NOTHING", rows);
		}

		public async Task SaveTransactions(Transaction[] transactions, uint256 blockId, DateTimeOffset? now)
		{
			var txs = transactions.Select(tx =>
			new {
				code = Network.CryptoCode,
				id = tx.GetHash().ToString(),
				raw = tx.ToBytes(),
				indexed_at = now is null ? default : now.Value,
			}).ToArray();
			if (now is null)
				await Connection.ExecuteAsync("INSERT INTO txs VALUES (@code, @id, @raw) ON CONFLICT (code, id) DO UPDATE SET raw = COALESCE(@raw, txs.raw)", txs);
			else
				await Connection.ExecuteAsync("INSERT INTO txs VALUES (@code, @id, @raw, @indexed_at) ON CONFLICT (code, id) DO UPDATE SET indexed_at=@indexed_at, raw = COALESCE(@raw, txs.raw)", txs);
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
		public async Task CreateDerivationLines(string walletId, string scheme, params (string lineName, KeyPathTemplate keyPathTemplate)[] lines)
		{
			var rows = lines.Select(c => new { code = Network.CryptoCode, walletId = walletId, scheme = scheme, c.lineName, keyPathTemplate = c.keyPathTemplate.ToString() }).ToArray();
			await Connection.ExecuteAsync("INSERT INTO derivation_lines VALUES (@code, @walletId, @scheme, @lineName, @keyPathTemplate) ON CONFLICT DO NOTHING", rows);
		}

		public async Task CreateDerivation(string walletId, string scheme)
		{
			await Connection.ExecuteAsync("INSERT INTO derivations VALUES (@code, @walletId, @scheme) ON CONFLICT DO NOTHING", new { code = Network.CryptoCode, walletId, scheme });
		}

		record DerivationLinesScriptPubKeyInsert(string code, string wallet_id, string scheme, string line_name, int idx, string keypath, string scriptpubkey);
		public async Task<int> GenerateAddresses(string walletId, string scheme, string line, GenerateAddressQuery? query)
		{
			query = query ?? new GenerateAddressQuery();

			var unused = await Connection.ExecuteScalarAsync<int>("SELECT COUNT(NOT used) FROM derivation_lines_scriptpubkeys WHERE code = @code AND wallet_id = @walletId AND scheme = @scheme AND line_name = @line", new { code = Network.CryptoCode, walletId, scheme, line });
			if (unused >= MinPoolSize)
				return 0;
			var toGenerate = Math.Max(0, MaxPoolSize - unused);
			if (query.MaxAddresses is int max)
				toGenerate = Math.Min(max, toGenerate);
			if (query.MinAddresses is int min)
				toGenerate = Math.Max(min, toGenerate);
			if (toGenerate == 0)
				return 0;

			var row = await Connection.QueryFirstAsync<(int next_index, string keypath_template)>("SELECT next_index, keypath_template FROM derivation_lines WHERE code=@code AND wallet_id=@walletId AND scheme=@scheme AND name=@line", new { code = Network.CryptoCode, walletId, scheme, line });
			var keypathTemplate = KeyPathTemplate.Parse(row.keypath_template);

			var legacyHeader = $"Legacy({Network.CryptoCode}):DERIVATIONSCHEME:";
			var legacyScheme = scheme;
			if (scheme.StartsWith(legacyHeader, StringComparison.OrdinalIgnoreCase))
				legacyScheme = scheme.Substring(legacyHeader.Length);
			var schemeObject = derivationStrategyFactory.Parse(legacyScheme);
			var derivationLine = schemeObject.GetLineFor(keypathTemplate);
			var scriptpubkeys = new Script[toGenerate];
			var linesScriptpubkeys = new DerivationLinesScriptPubKeyInsert[toGenerate];
			Parallel.For(row.next_index, row.next_index + toGenerate, i =>
			{
				var derivation = derivationLine.Derive((uint)i);
				scriptpubkeys[i - row.next_index] = derivation.ScriptPubKey;
				linesScriptpubkeys[i - row.next_index] = new DerivationLinesScriptPubKeyInsert(Network.CryptoCode, walletId, scheme, line, i, keypathTemplate.GetKeyPath(i, false).ToString(), derivation.ScriptPubKey.ToHex());
			});
			await AddScriptPubKeys(walletId, scriptpubkeys);
			return await Connection.ExecuteAsync("INSERT INTO derivation_lines_scriptpubkeys VALUES (@code, @wallet_id, @scheme, @line_name, @idx, @keypath, @scriptpubkey) ON CONFLICT DO NOTHING", linesScriptpubkeys);
		}
	}
}
