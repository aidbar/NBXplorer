using NBitcoin;
using Dapper;
using NBitcoin.Altcoins;
using NBXplorer.Configuration;
using NBXplorer.DerivationStrategy;
using NBXplorer.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NBitcoin.DataEncoders;
using System.Data.Common;
using NBXplorer.Logging;
using Microsoft.Extensions.Logging;
using Npgsql;
using NBitcoin.RPC;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using NBitcoin.Crypto;

namespace NBXplorer
{
	public class PostgresRepositoryProvider : IRepositoryProvider
	{
		Dictionary<string, PostgresRepository> _Repositories = new Dictionary<string, PostgresRepository>();
		ExplorerConfiguration _Configuration;

		public Task StartCompletion => Task.CompletedTask;

		public NBXplorerNetworkProvider Networks { get; }
		public DbConnectionFactory ConnectionFactory { get; }
		public KeyPathTemplates KeyPathTemplates { get; }

		public PostgresRepositoryProvider(NBXplorerNetworkProvider networks,
			ExplorerConfiguration configuration,
			DbConnectionFactory connectionFactory,
			KeyPathTemplates keyPathTemplates)
		{
			Networks = networks;
			_Configuration = configuration;
			ConnectionFactory = connectionFactory;
			KeyPathTemplates = keyPathTemplates;
		}
		public IRepository GetRepository(string cryptoCode)
		{
			_Repositories.TryGetValue(cryptoCode, out PostgresRepository repository);
			return repository;
		}
		public IRepository GetRepository(NBXplorerNetwork network)
		{
			return GetRepository(network.CryptoCode);
		}

		public async Task StartAsync(CancellationToken cancellationToken)
		{
			foreach (var net in Networks.GetAll())
			{
				var settings = GetChainSetting(net);
				if (settings != null)
				{
					var repo = net.NBitcoinNetwork.NetworkSet == Liquid.Instance ? throw new NotSupportedException() :
						new PostgresRepository(ConnectionFactory, net, KeyPathTemplates, settings.RPC);
					repo.MaxPoolSize = _Configuration.MaxGapSize;
					repo.MinPoolSize = _Configuration.MinGapSize;
					repo.MinUtxoValue = settings.MinUtxoValue;
					_Repositories.Add(net.CryptoCode, repo);
				}
			}
			foreach (var repo in _Repositories.Select(kv => kv.Value))
			{
				if (GetChainSetting(repo.Network) is ChainConfiguration chainConf &&
				chainConf.Rescan &&
				(chainConf.RescanIfTimeBefore is null || chainConf.RescanIfTimeBefore.Value >= DateTimeOffset.UtcNow))
				{
					Logs.Configuration.LogInformation($"{repo.Network.CryptoCode}: Rescanning the chain...");
					await repo.SetIndexProgress(null);
				}
			}
			if (_Configuration.TrimEvents > 0)
			{
				Logs.Explorer.LogInformation("Trimming the event table if needed...");
				int trimmed = 0;
				foreach (var repo in _Repositories.Select(kv => kv.Value))
				{
					if (GetChainSetting(repo.Network) is ChainConfiguration chainConf)
					{
						trimmed += await repo.TrimmingEvents(_Configuration.TrimEvents, cancellationToken);
					}
				}
				if (trimmed != 0)
					Logs.Explorer.LogInformation($"Trimmed {trimmed} events in total...");
			}
		}
		private ChainConfiguration GetChainSetting(NBXplorerNetwork net)
		{
			return _Configuration.ChainConfigurations.FirstOrDefault(c => c.CryptoCode == net.CryptoCode);
		}

		public Task StopAsync(CancellationToken cancellationToken)
		{
			return Task.CompletedTask;
		}
	}
	public class PostgresRepository : IRepository
	{
		private DbConnectionFactory connectionFactory;
		private readonly RPCClient rpc;

		public DbConnectionFactory ConnectionFactory => connectionFactory;
		public PostgresRepository(DbConnectionFactory connectionFactory, NBXplorerNetwork network, KeyPathTemplates keyPathTemplates, RPCClient rpc)
		{
			this.connectionFactory = connectionFactory;
			Network = network;
			KeyPathTemplates = keyPathTemplates;
			this.rpc = rpc;
			Serializer = new Serializer(network);
		}

		public int BatchSize { get; set; }

		public int MaxPoolSize { get; set; }
		public int MinPoolSize { get; set; }
		public Money MinUtxoValue { get; set; }

		public NBXplorerNetwork Network { get; set; }
		public KeyPathTemplates KeyPathTemplates { get; }
		public Serializer Serializer { get; set; }

		public async Task CancelReservation(DerivationStrategyBase strategy, KeyPath[] keyPaths)
		{
			await using var conn = await GetConnection();
			var parameters = keyPaths
				.Select(o =>
				{
					var template = KeyPathTemplates.GetKeyPathTemplate(o);
					var descriptor = GetDescriptorKey(strategy, KeyPathTemplates.GetDerivationFeature(o));
					return new
					{
						descriptor.code,
						descriptor.descriptor,
						idx = (int)template.GetIndex(o)
					};
				})
				.ToList();
			await conn.Connection.ExecuteAsync(
				"UPDATE descriptors_scripts SET used='f' WHERE code=@code AND descriptor=@descriptor AND idx=@idx", parameters);
		}

		public TrackedTransaction CreateTrackedTransaction(TrackedSource trackedSource, TrackedTransactionKey transactionKey, IEnumerable<Coin> coins, Dictionary<Script, KeyPath> knownScriptMapping)
		{
			return new TrackedTransaction(transactionKey, trackedSource, coins, knownScriptMapping);
		}

		public TrackedTransaction CreateTrackedTransaction(TrackedSource trackedSource, TrackedTransactionKey transactionKey, Transaction tx, Dictionary<Script, KeyPath> knownScriptMapping)
		{
			return new TrackedTransaction(transactionKey, trackedSource, tx, knownScriptMapping);
		}

		public ValueTask<int> DefragmentTables(CancellationToken cancellationToken = default)
		{
			return default;
		}

		public record DescriptorKey(string code, string descriptor);
		internal DescriptorKey GetDescriptorKey(DerivationStrategyBase strategy, DerivationFeature derivationFeature)
		{
			var hash = Encoders.Hex.EncodeData(Hashes.RIPEMD160(new UTF8Encoding(false).GetBytes($"{Network.CryptoCode}|{strategy}|{derivationFeature}")));
			return new DescriptorKey(Network.CryptoCode, hash);
		}
		// metadata isn't really part of the key, but it's handy to have it here when we do INSERT INTO wallets.
		public record WalletKey(string wid, string metadata);
		WalletKey GetWalletKey(DerivationStrategyBase strategy)
		{
			var hash = Encoders.Hex.EncodeData(Hashes.RIPEMD160(new UTF8Encoding(false).GetBytes($"{Network.CryptoCode}|{strategy}")));
			JObject m = new JObject();
			m.Add(new JProperty("type", new JValue("NBXv1-Derivation")));
			m.Add(new JProperty("code", new JValue(Network.CryptoCode)));
			m.Add(new JProperty("derivation", new JValue(strategy.ToString())));
			return new WalletKey(hash, m.ToString(Formatting.None));
		}
		WalletKey GetWalletKey(IDestination destination)
		{
			var address = destination.ScriptPubKey.GetDestinationAddress(Network.NBitcoinNetwork);
			var hash = Encoders.Hex.EncodeData(Hashes.RIPEMD160(new UTF8Encoding(false).GetBytes(Network.CryptoCode + "|" + address.ToString())));
			JObject m = new JObject();
			m.Add(new JProperty("type", new JValue("NBXv1-Address")));
			m.Add(new JProperty("code", new JValue(Network.CryptoCode)));
			m.Add(new JProperty("address", new JValue(address.ToString())));
			return new WalletKey(hash, m.ToString(Formatting.None));
		}
		internal WalletKey GetWalletKey(TrackedSource source)
		{
			if (source is null)
				throw new ArgumentNullException(nameof(source));
			return source switch
			{
				DerivationSchemeTrackedSource derivation => GetWalletKey(derivation.DerivationStrategy),
				AddressTrackedSource addr => GetWalletKey(addr.Address),
				_ => throw new NotSupportedException(source.GetType().ToString())
			};
		}

		record DescriptorScriptInsert(string code, string descriptor, int idx, string script, string redeem, string addr, string walletid);
		public async Task<int> GenerateAddresses(DerivationStrategyBase strategy, DerivationFeature derivationFeature, GenerateAddressQuery query = null)
		{
			await using var connection = await connectionFactory.CreateConnection();
			query = query ?? new GenerateAddressQuery();

			var descriptorKey = GetDescriptorKey(strategy, derivationFeature);
			var walletKey = GetWalletKey(strategy);
			var gap = await connection.ExecuteScalarAsync<int>(
				"SELECT gap FROM descriptors " +
				"WHERE code=@code AND descriptor=@descriptor", descriptorKey);
			if (gap >= MinPoolSize)
				return 0;
			var toGenerate = Math.Max(0, MaxPoolSize - gap);
			if (query.MaxAddresses is int max)
				toGenerate = Math.Min(max, toGenerate);
			if (query.MinAddresses is int min)
				toGenerate = Math.Max(min, toGenerate);
			if (toGenerate == 0)
				return 0;
			var keyTemplate = KeyPathTemplates.GetKeyPathTemplate(derivationFeature);
			retry:
			var row = await connection.ExecuteScalarAsync<int?>("SELECT next_idx FROM descriptors WHERE code=@code AND descriptor=@descriptor", descriptorKey);
			if (row is null)
			{
				await connection.ExecuteAsync("INSERT INTO wallets VALUES (@wid, @metadata::JSONB) ON CONFLICT DO NOTHING", walletKey);
				await connection.ExecuteAsync("INSERT INTO descriptors VALUES (@code, @descriptor, @metadata::JSONB) ON CONFLICT DO NOTHING;", new
				{
					descriptorKey.code,
					descriptorKey.descriptor,
					metadata = Serializer.ToString(new LegacyDescriptorMetadata()
					{
						Derivation = strategy,
						Feature = derivationFeature,
						KeyPathTemplate = keyTemplate,
						Type = LegacyDescriptorMetadata.TypeName
					})
				});
				goto retry;
			}
			if (row is null)
				return 0;
			var nextIndex = row.Value;
			var line = strategy.GetLineFor(keyTemplate);
			var scriptpubkeys = new Script[toGenerate];
			var linesScriptpubkeys = new DescriptorScriptInsert[toGenerate];
			Parallel.For(nextIndex, nextIndex + toGenerate, i =>
			{
				var derivation = line.Derive((uint)i);
				scriptpubkeys[i - nextIndex] = derivation.ScriptPubKey;
				linesScriptpubkeys[i - nextIndex] = new DescriptorScriptInsert(
					descriptorKey.code,
					descriptorKey.descriptor,
					i,
					derivation.ScriptPubKey.ToHex(),
					derivation.Redeem?.ToHex() is string r ? $"{{\"redeem\":\"{r}\"}}" : null,
					derivation.ScriptPubKey.GetDestinationAddress(Network.NBitcoinNetwork).ToString(),
					walletKey.wid);
			});

			await connection.ExecuteAsync(
				"INSERT INTO scripts VALUES (@code, @script, @addr) ON CONFLICT DO NOTHING;" +
				"INSERT INTO descriptors_scripts VALUES (@code, @descriptor, @idx, @script, @redeem::JSONB) ON CONFLICT DO NOTHING;" +
				"INSERT INTO wallets_scripts VALUES(@code, @script, @walletid, @descriptor, @idx) ON CONFLICT DO NOTHING;", linesScriptpubkeys);
			return toGenerate;
		}

		public Task<int> GenerateAddresses(DerivationStrategyBase strategy, DerivationFeature derivationFeature, int maxAddresses)
		{
			return GenerateAddresses(strategy, derivationFeature, new GenerateAddressQuery(null, maxAddresses));
		}

		public async Task<IList<NewEventBase>> GetEvents(long lastEventId, int? limit = null)
		{
			await using var connection = await connectionFactory.CreateConnection();
			var limitClause = string.Empty;
			if (limit is int i && i > 0)
				limitClause = $" LIMIT {i}";
			var res = (await connection.QueryAsync<(long id, string data)>($"SELECT id, data FROM nbxv1_evts WHERE code=@code AND id > @lastEventId ORDER BY id{limitClause}", new { code = Network.CryptoCode, lastEventId }))
				.Select(ToTypedEvent)
				.ToArray();
			return res;
		}

		private NewEventBase ToTypedEvent((long id, string data) r)
		{
			var ev = NewEventBase.ParseEvent(r.data, Serializer.Settings);
			ev.EventId = r.id;
			return ev;
		}

		public async Task<IList<NewEventBase>> GetLatestEvents(int limit = 10)
		{
			await using var connection = await connectionFactory.CreateConnection();
			var limitClause = string.Empty;
			if (limit is int i && i > 0)
				limitClause = $" LIMIT {i}";
			var res = (await connection.QueryAsync<(long id, string data)>($"SELECT id, data FROM nbxv1_evts WHERE code=@code ORDER BY id DESC{limitClause}", new { code = Network.CryptoCode }))
				.Select(ToTypedEvent)
				.ToArray();
			Array.Reverse(res);
			return res;
		}

		public async Task<MultiValueDictionary<Script, KeyPathInformation>> GetKeyInformations(IList<Script> scripts)
		{
			await using var connection = await connectionFactory.CreateConnection();
			return await GetKeyInformations(connection, scripts);
		}
		async Task<MultiValueDictionary<Script, KeyPathInformation>> GetKeyInformations(DbConnection connection, IList<Script> scripts)
		{
			MultiValueDictionary<Script, KeyPathInformation> result = new MultiValueDictionary<Script, KeyPathInformation>();
			foreach (var s in scripts)
				result.AddRange(s, Array.Empty<KeyPathInformation>());
			var command = connection.CreateCommand();
			StringBuilder builder = new StringBuilder();
			builder.Append("SELECT ts.script, ts.addr, ts.derivation, ts.keypath, ts.redeem FROM ( VALUES ");
			int idx = 0;
			foreach (var s in scripts)
			{
				if (idx != 0)
					builder.Append(',');
				builder.Append($"('{Network.CryptoCode}', '{s.ToHex()}')");
				idx++;
			}
			if (idx == 0)
				return result;
			builder.Append(") r (code, script)," +
				" LATERAL (" +
				"	SELECT ws.script, s.addr, d.metadata->>'derivation' derivation, nbxv1_get_keypath(d.metadata, ds.idx) keypath, ds.metadata->>'redeem' redeem " +
				"	FROM wallets_scripts ws " +
				"	LEFT JOIN descriptors_scripts ds USING (code, descriptor, idx) " +
				"   LEFT JOIN descriptors d USING (code, descriptor) " +
				"	JOIN scripts s ON s.code=ws.code AND s.script=ws.script " +
				"   WHERE ws.code=r.code AND ws.script=r.script) ts;");
			command.CommandText = builder.ToString();
			await using var reader = await command.ExecuteReaderAsync();
			while (await reader.ReadAsync())
			{
				bool isExplicit = reader.IsDBNull(3);
				bool isDescriptor = !isExplicit;
				var script = Script.FromHex(reader.GetString(0));
				var derivationStrategy = isDescriptor ? Network.DerivationStrategyFactory.Parse(reader.GetString(2)) : null;
				var addr = BitcoinAddress.Create(reader.GetString(1), Network.NBitcoinNetwork);
				var keypath = isDescriptor ? KeyPath.Parse(reader.GetString(3)) : null;
				var redeem = reader.IsDBNull(4) ? null : reader.GetString(4);
				result.Add(script, new KeyPathInformation()
				{
					Address = addr,
					DerivationStrategy = isDescriptor ? derivationStrategy : null,
					KeyPath = isDescriptor ? keypath : null,
					ScriptPubKey = script,
					TrackedSource = isDescriptor && derivationStrategy is not null ? new DerivationSchemeTrackedSource(derivationStrategy) :
									isExplicit ? new AddressTrackedSource(addr) : null,
					Feature = keypath is null ? DerivationFeature.Deposit : KeyPathTemplates.GetDerivationFeature(keypath),
					Redeem = redeem is null ? null : Script.FromHex(redeem)
				});
			}
			return result;
		}

		internal LegacyDescriptorMetadata GetDescriptorMetadata(string str)
		{
			var o = JObject.Parse(str);
			if (o["type"].Value<string>() != LegacyDescriptorMetadata.TypeName)
				return null;
			return this.Serializer.ToObject<LegacyDescriptorMetadata>(o);
		}

		FixedSizeCache<uint256, uint256> noMatchCache = new FixedSizeCache<uint256, uint256>(5000, k => k);

		record ScriptPubKeyQuery(string code, string id);

		public async Task<TrackedTransaction[]> GetMatches(Block block, SlimChainedBlock slimBlock, DateTimeOffset now, bool useCache)
		{
			var matches = await GetMatches(block.Transactions, slimBlock, now, useCache);
			if (matches.Length > 0)
			{
				var blockIndexes = block.Transactions.Select((tx, i) => (tx, i))
								  .ToDictionary(o => o.tx.GetHash(), o => o.i);
				foreach (var match in matches)
					match.BlockIndex = blockIndexes[match.TransactionHash];
			}
			return matches;
		}

		public async Task<TrackedTransaction[]> GetMatches(IList<Transaction> txs, SlimChainedBlock slimBlock, DateTimeOffset now, bool useCache)
		{
			foreach (var tx in txs)
				tx.PrecomputeHash(false, true);

			var outputCount = txs.Select(tx => tx.Outputs.Count).Sum();
			var inputCount = txs.Select(tx => tx.Inputs.Count).Sum();
			var outpointCount = inputCount + outputCount;

			var scripts = new List<Script>(outpointCount);
			var transactionsPerScript = new MultiValueDictionary<Script, NBitcoin.Transaction>(outpointCount);

			var matches = new Dictionary<string, TrackedTransaction>();
			var noMatchTransactions = slimBlock?.Hash is null ? new HashSet<uint256>(txs.Count) : null;
			var transactions = new Dictionary<uint256, NBitcoin.Transaction>(txs.Count);
			var outpoints = new List<OutPoint>(inputCount);


			var outCount = txs.Select(t => t.Outputs.Count).Sum();
			List<DbConnectionHelper.NewOut> outs = new List<DbConnectionHelper.NewOut>(outCount);
			var inCount = txs.Select(t => t.Inputs.Count).Sum();
			List<DbConnectionHelper.NewIn> ins = new List<DbConnectionHelper.NewIn>(inCount);

			foreach (var tx in txs)
			{
				if (!transactions.TryAdd(tx.GetHash(), tx))
					continue;
				if (slimBlock?.Hash != null && useCache && noMatchCache.Contains(tx.GetHash()))
				{
					continue;
				}
				noMatchTransactions?.Add(tx.GetHash());
				if (!tx.IsCoinBase)
				{
					int i = 0;
					foreach (var input in tx.Inputs)
					{
						ins.Add(new DbConnectionHelper.NewIn(tx.GetHash(), i, input.PrevOut.Hash, (int)input.PrevOut.N));
						i++;
					}
				}
				int io = -1;
				foreach (var output in tx.Outputs)
				{
					io++;
					if (MinUtxoValue != null && output.Value < MinUtxoValue)
						continue;
					outs.Add(new DbConnectionHelper.NewOut(tx.GetHash(), io, output.ScriptPubKey, output.Value)); 
				}
			}

			await using var connection = await connectionFactory.CreateConnectionHelper(Network);
			await connection.FetchMatches(outs, ins);
			var result = await connection.Connection.QueryMultipleAsync(
				"SELECT * FROM matched_outs;" +
				"SELECT * FROM matched_ins");
			var matchedOuts = await result.ReadAsync();
			var matchedIns = await result.ReadAsync();

			foreach (var r in matchedOuts)
			{
				var s = Script.FromHex(r.script);
				scripts.Add(s);
				transactionsPerScript.Add(s, transactions[uint256.Parse(r.tx_id)]);
			}
			foreach (var r in matchedIns)
			{
				var s = Script.FromHex(r.script);
				scripts.Add(s);
				transactionsPerScript.Add(s, transactions[uint256.Parse(r.tx_id)]);
			}
			if (scripts.Count > 0)
			{
				var keyInformations = await GetKeyInformations(connection.Connection, scripts);
				foreach (var keyInfoByScripts in keyInformations)
				{
					foreach (var tx in transactionsPerScript[keyInfoByScripts.Key])
					{
						if (keyInfoByScripts.Value.Count != 0)
							noMatchTransactions?.Remove(tx.GetHash());
						foreach (var keyInfo in keyInfoByScripts.Value)
						{
							var matchesGroupingKey = $"{keyInfo.DerivationStrategy?.ToString() ?? keyInfo.ScriptPubKey.ToHex()}-[{tx.GetHash()}]";
							if (!matches.TryGetValue(matchesGroupingKey, out TrackedTransaction match))
							{
								match = CreateTrackedTransaction(keyInfo.TrackedSource,
									new TrackedTransactionKey(tx.GetHash(), slimBlock?.Hash, false),
									tx,
									new Dictionary<Script, KeyPath>());
								match.BlockHeight = slimBlock?.Height;
								match.FirstSeen = now;
								match.Inserted = now;
								matches.Add(matchesGroupingKey, match);
							}
							if (keyInfo.KeyPath != null)
								match.KnownKeyPathMapping.TryAdd(keyInfo.ScriptPubKey, keyInfo.KeyPath);
						}
					}
				}
				foreach (var m in matches.Values)
				{
					m.KnownKeyPathMappingUpdated();
				}
			}
			if (noMatchTransactions != null)
			{
				foreach (var txId in noMatchTransactions)
				{
					noMatchCache.Add(txId);
				}
			}
			return matches.Values.Count == 0 ? Array.Empty<TrackedTransaction>() : matches.Values.ToArray();
		}

		public Task<TrackedTransaction[]> GetMatches(Transaction tx, SlimChainedBlock slimBlock, DateTimeOffset now, bool useCache)
		{
			return GetMatches(new[] { tx }, slimBlock, now, useCache);
		}

		public async Task<Dictionary<OutPoint, TxOut>> GetOutPointToTxOut(IList<OutPoint> outPoints)
		{
			await using var connection = await connectionFactory.CreateConnectionHelper(Network);
			return await connection.GetOutputs(outPoints);
		}

		record SavedTransactionRow(byte[] raw, string blk_id, DateTime seen_at);
		public async Task<Repository.SavedTransaction[]> GetSavedTransactions(uint256 txid)
		{
			await using var connection = await connectionFactory.CreateConnectionHelper(Network);
			var tx = await connection.Connection.QueryFirstOrDefaultAsync<SavedTransactionRow>("SELECT raw, blk_id, seen_at FROM txs WHERE code=@code AND tx_id=@tx_id", new { code = Network.CryptoCode, tx_id = txid.ToString() });
			if (tx?.raw is null)
				return Array.Empty<Repository.SavedTransaction>();
			return new[] { new Repository.SavedTransaction()
			{
				BlockHash = tx.blk_id is null ? null : uint256.Parse(tx.blk_id),
				Timestamp = new DateTimeOffset(tx.seen_at),
				Transaction = Transaction.Load(tx.raw, Network.NBitcoinNetwork)
			}};
		}

		public async Task<TrackedTransaction[]> GetTransactions(TrackedSource trackedSource, uint256 txId = null, bool includeTransactions = true, CancellationToken cancellation = default)
		{
			await using var connection = await connectionFactory.CreateConnectionHelper(Network);
			var tip = await connection.GetTip();
			if (tip is null)
				return Array.Empty<TrackedTransaction>();
			var utxos = await
				connection.Connection.QueryAsync<(string tx_id, long idx, string block_id, bool is_out, string spent_tx_id, long spent_idx, string script, long value, bool immature, string keypath, DateTime seen_at)>(
				"SELECT io.tx_id, io.idx, io.blk_id, io.is_out, io.spent_tx_id, io.spent_idx, io.script, io.value, io.immature, nbxv1_get_keypath(d.metadata, ds.idx) keypath, io.seen_at " +
				"FROM wallets_scripts ws " +
				"LEFT JOIN descriptors_scripts ds USING (code, descriptor, idx) " +
				"LEFT JOIN descriptors d USING (code, descriptor) " +
				"JOIN ins_outs io ON io.code=ws.code AND io.script=ws.script " +
				"WHERE ws.code=@code AND ws.wallet_id=@walletId", new { code = Network.CryptoCode, walletId = GetWalletKey(trackedSource).wid });
			utxos.TryGetNonEnumeratedCount(out int c);
			var trackedById = new Dictionary<string, TrackedTransaction>(c);
			var txIdStr = txId?.ToString();
			foreach (var utxo in utxos)
			{
				if (txIdStr != null && utxo.tx_id != txIdStr)
					continue;
				var tracked = GetTrackedTransaction(trackedSource, utxo.tx_id, utxo.block_id, utxo.seen_at, trackedById);
				if (utxo.is_out)
				{
					var txout = Network.NBitcoinNetwork.Consensus.ConsensusFactory.CreateTxOut();
					txout.Value = Money.Satoshis(utxo.value);
					txout.ScriptPubKey = Script.FromHex(utxo.script);
					tracked.ReceivedCoins.Add(new Coin(new OutPoint(tracked.Key.TxId, (uint)utxo.idx), txout));
					tracked.IsCoinBase = utxo.immature;
					if (utxo.keypath is string)
						tracked.KnownKeyPathMapping.Add(txout.ScriptPubKey, KeyPath.Parse(utxo.keypath));
				}
				else
				{
					tracked.SpentOutpoints.Add(new OutPoint(uint256.Parse(utxo.spent_tx_id), (uint)utxo.spent_idx));
				}
			}

			var txsToFetch = includeTransactions ? trackedById.Keys.AsList() :
												  // For double spend detection, we need the full transactions from unconfs
												  trackedById.Where(t => t.Value.BlockHash is null).Select(t => t.Key).AsList();
			var txRaws = await connection.Connection.QueryAsync<(string tx_id, byte[] raw)>(
				"SELECT	tx_id, raw FROM txs WHERE code=@code AND tx_id=ANY(@txId) AND raw IS NOT NULL;", new { code = Network.CryptoCode, txId = txsToFetch });
			foreach (var row in txRaws)
			{
				var tracked = trackedById[row.tx_id];
				tracked.Transaction = Transaction.Load(row.raw, Network.NBitcoinNetwork);
				tracked.Key = new TrackedTransactionKey(tracked.Key.TxId, tracked.Key.BlockHash, false);
				if (tracked.BlockHash is null) // Only need the spend outpoint for double spend detection on unconf txs
					tracked.SpentOutpoints.AddRange(tracked.Transaction.Inputs.Select(o => o.PrevOut));
			}

			return trackedById.Values.Select(c =>
			{
				c.KnownKeyPathMappingUpdated();
				return c;
			}).ToArray();
		}

		private TrackedTransaction GetTrackedTransaction(TrackedSource trackedSource, string tx_id, string block_id, DateTime seenAt, Dictionary<string, TrackedTransaction> trackedById)
		{
			if (trackedById.TryGetValue(tx_id, out var tracked))
				return tracked;
			TrackedTransactionKey key = new TrackedTransactionKey(uint256.Parse(tx_id), block_id is null ? null : uint256.Parse(block_id), true);
			tracked = CreateTrackedTransaction(trackedSource, key, null as IEnumerable<Coin>, new Dictionary<Script, KeyPath>());
			tracked.FirstSeen = seenAt;
			trackedById.Add(tx_id, tracked);
			return tracked;
		}

		public async Task<KeyPathInformation> GetUnused(DerivationStrategyBase strategy, DerivationFeature derivationFeature, int n, bool reserve)
		{
			await using var helper = await connectionFactory.CreateConnectionHelper(Network);
			var connection = helper.Connection;
			var key = GetDescriptorKey(strategy, derivationFeature);
			retry:
			var unused = await connection.QueryFirstOrDefaultAsync(
				"SELECT s.script, s.addr, nbxv1_get_keypath(d.metadata , ds.idx) keypath, ds.metadata->>'redeem' redeem FROM descriptors_scripts ds " +
				"JOIN scripts s USING (code, script) " +
				"JOIN descriptors d USING (code, descriptor) " +
				"WHERE ds.code=@code AND ds.descriptor=@descriptor AND ds.used='f' " +
				"LIMIT 1 OFFSET @skip", new { key.code, key.descriptor, skip = n });
			if (unused is null)
				return null;
			if (reserve)
			{
				var updated = await connection.ExecuteAsync("UPDATE descriptors_scripts SET used='t' WHERE code=@code AND script=@script AND descriptor=@descriptor AND used='f'", new { key.code, unused.script, key.descriptor });
				if (updated == 0)
					goto retry;
			}
			var keypath = KeyPath.Parse(unused.keypath);
			var keyInfo = new KeyPathInformation()
			{
				Address = BitcoinAddress.Create(unused.addr, Network.NBitcoinNetwork),
				DerivationStrategy = strategy,
				KeyPath = keypath,
				ScriptPubKey = Script.FromHex(unused.script),
				TrackedSource = new DerivationSchemeTrackedSource(strategy),
				Feature = KeyPathTemplates.GetDerivationFeature(keypath),
				Redeem = unused.redeem is string s ? Script.FromHex(s) : null
			};
			await ImportAddressToRPC(helper, keyInfo.TrackedSource, keyInfo.Address, keyInfo.KeyPath);
			return keyInfo;
		}

		record KeyInfoInsert(string code, string descriptor, string script, string address, long? idx, string walletid, string redeem);
		public async Task SaveKeyInformations(KeyPathInformation[] keyPathInformations)
		{
			await using var connection = await connectionFactory.CreateConnection();
			await SaveKeyInformations(connection, keyPathInformations);
		}

		internal async Task SaveKeyInformations(DbConnection connection, KeyPathInformation[] keyPathInformations)
		{
			var inserts = new List<KeyInfoInsert>();
			foreach (var ki in keyPathInformations)
			{
				var descriptorKey = ki.TrackedSource is DerivationSchemeTrackedSource a ? GetDescriptorKey(a.DerivationStrategy, ki.Feature) : null;
				var wid = GetWalletKey(ki.TrackedSource).wid;
				if (descriptorKey is not null)
				{
					inserts.Add(new KeyInfoInsert(
						Network.CryptoCode,
						descriptorKey.descriptor,
						ki.ScriptPubKey.ToHex(),
						ki.Address.ToString(),
						ki.GetIndex(),
						wid,
						ki.Redeem is null ? null : $"{{\"redeem\":\"{ki.Redeem.ToHex()}\"}}"));
				}
				else
				{
					inserts.Add(new KeyInfoInsert(
						Network.CryptoCode,
						null,
						ki.ScriptPubKey.ToHex(),
						ki.Address.ToString(),
						null,
						wid,
						null));
				}
			}

			var hdParameters = inserts.Where(i => i.descriptor is not null).ToList();
			await connection.ExecuteAsync(
				"INSERT INTO scripts VALUES (@code, @script, @address) ON CONFLICT DO NOTHING;" +
				"INSERT INTO descriptors_scripts VALUES (@code, @descriptor, @idx, @script, @redeem::JSONB, 't') ON CONFLICT (code, descriptor, idx) DO UPDATE SET used='t';" +
				"INSERT INTO wallets_scripts VALUES (@code, @script, @walletid, @descriptor, @idx) ON CONFLICT DO NOTHING;", hdParameters);

			var singleParameters = inserts.Where(i => i.descriptor is null).ToList();
			await connection.ExecuteAsync(
				"INSERT INTO scripts VALUES (@code, @script, @address) ON CONFLICT DO NOTHING;" +
				"INSERT INTO wallets_scripts VALUES (@code, @script, @walletid, @descriptor, @idx) ON CONFLICT DO NOTHING;", singleParameters);
		}

		private async Task ImportAddressToRPC(DbConnectionHelper connection, TrackedSource trackedSource, BitcoinAddress address, KeyPath keyPath)
		{
			var k = GetWalletKey(trackedSource);
			var shouldImportRPC = (await connection.GetMetadata<string>(k.wid, WellknownMetadataKeys.ImportAddressToRPC)).AsBoolean();
			if (!shouldImportRPC)
				return;
			var accountKey = await connection.GetMetadata<BitcoinExtKey>(k.wid, WellknownMetadataKeys.AccountHDKey);
			await ImportAddressToRPC(accountKey, address, keyPath);
		}
		private async Task ImportAddressToRPC(BitcoinExtKey accountKey, BitcoinAddress address, KeyPath keyPath)
		{
			if (accountKey != null)
			{
				await rpc.ImportPrivKeyAsync(accountKey.Derive(keyPath).PrivateKey.GetWif(Network.NBitcoinNetwork), null, false);
			}
			else
			{
				try
				{
					await rpc.ImportAddressAsync(address, null, false);
				}
				catch (RPCException) // Probably the private key has already been imported
				{

				}
			}
		}

		public ValueTask<bool> MigrateOutPoints(string directory, CancellationToken cancellationToken = default)
		{
			return default;
		}

		public ValueTask<int> MigrateSavedTransactions(CancellationToken cancellationToken = default)
		{
			return default;
		}

		public Task Ping()
		{
			return Task.CompletedTask;
		}

		public async Task Prune(TrackedSource trackedSource, IEnumerable<TrackedTransaction> prunable)
		{
			if (prunable.TryGetNonEnumeratedCount(out var c) && c == 0)
				return;
			await using var helper = await GetConnection();
			var receivedCoinsToDelete =
				prunable
				.Where(p => p.BlockHash is not null)
				.SelectMany(c => c.ReceivedCoins)
				.Select(c => new
				{
					code = Network.CryptoCode,
					txId = c.Outpoint.Hash.ToString(),
					idx = (long)c.Outpoint.N
				}).ToArray();
			var spentCoins =
				prunable
				.Where(p => p.BlockHash is not null)
				.SelectMany(c => c.SpentOutpoints)
				.Select(c => new
				{
					code = Network.CryptoCode,
					txId = c.Hash.ToString(),
					idx = (long)c.N
				}).ToArray();
			await helper.Connection.ExecuteAsync("DELETE FROM outs WHERE code=@code AND tx_id=@txId AND idx=@idx", receivedCoinsToDelete);
			await helper.Connection.ExecuteAsync("DELETE FROM ins WHERE code=@code AND spent_tx_id=@txId AND spent_idx=@idx", spentCoins);

			var mempoolPrunable =
				prunable
				.Where(p => p.BlockHash is null)
				.Select(p => new { code = Network.CryptoCode, txId = p.TransactionHash.ToString() })
				.ToArray();

			await helper.Connection.ExecuteAsync("UPDATE txs SET mempool='f' WHERE code=@code AND tx_id=@txId", mempoolPrunable);
		}

		public async Task<long> SaveEvent(NewEventBase evt)
		{
			await using var helper = await GetConnection();
			var json = evt.ToJObject(Serializer.Settings).ToString();
			var id = helper.Connection.ExecuteScalar<long>(
				"WITH cte AS (" +
				"INSERT INTO nbxv1_evts_ids AS ei VALUES (@code, 1) ON CONFLICT (code) DO UPDATE SET curr_id=ei.curr_id+1" +
				"RETURNING curr_id" +
				")" +
				"INSERT INTO nbxv1_evts (code, id, type, data) VALUES (@code, (SELECT * FROM cte), @type, @data::json) RETURNING id", new { code = Network.CryptoCode, type = evt.EventType, data = json });
			return id;
		}

		public async Task SaveMatches(TrackedTransaction[] transactions)
		{
			if (transactions.Length is 0)
				return;
			await using var helper = await connectionFactory.CreateConnectionHelper(Network);
			var connection = helper.Connection;

			var outCount = transactions.Select(t => t.ReceivedCoins.Count).Sum();
			var inCount = transactions.Select(t => t.SpentOutpoints.Count).Sum();
			List<DbConnectionHelper.NewOut> outs = new List<DbConnectionHelper.NewOut>(outCount);
			List<DbConnectionHelper.NewIn> ins = new List<DbConnectionHelper.NewIn>(inCount);
			foreach (var tx in transactions)
			{
				if (!tx.IsCoinBase)
				{
					foreach (var input in tx.SpentOutpoints)
					{
						ins.Add(new DbConnectionHelper.NewIn(
							tx.TransactionHash,
							tx.IndexOfInput(input),
							input.Hash,
							(int)input.N
							));
					}
				}
				
				foreach (var output in tx.GetReceivedOutputs())
				{
					outs.Add(new DbConnectionHelper.NewOut(
						tx.TransactionHash,
						output.Index,
						output.ScriptPubKey,
						(Money)output.Value
						));
				}
			}
			await helper.FetchMatches(outs, ins);
			await helper.SaveTransactions(transactions.Select(t => (t.Transaction, t.TransactionHash, t.BlockHash, t.BlockIndex, t.BlockHeight, t.IsCoinBase)), null);
			await helper.Connection.ExecuteAsync("CALL save_matches(@code)", new { code = Network.CryptoCode });
		}

		public async Task SaveMetadata<TMetadata>(TrackedSource source, string key, TMetadata value) where TMetadata : class
		{
			await using var helper = await connectionFactory.CreateConnectionHelper(Network);
			var walletKey = GetWalletKey(source);
			if (!await helper.SetMetadata(walletKey.wid, key, value))
			{
				await helper.Connection.ExecuteAsync("INSERT INTO wallets VALUES (@wid, @metadata::JSONB) ON CONFLICT DO NOTHING", walletKey);
				await helper.SetMetadata(walletKey.wid, key, value);
			}
		}

		public async Task<TMetadata> GetMetadata<TMetadata>(TrackedSource source, string key) where TMetadata : class
		{
			await using var helper = await connectionFactory.CreateConnectionHelper(Network);
			var walletKey = GetWalletKey(source);
			return await helper.GetMetadata<TMetadata>(walletKey.wid, key);
		}

		public async Task<List<Repository.SavedTransaction>> SaveTransactions(DateTimeOffset now, Transaction[] transactions, SlimChainedBlock slimBlock)
		{
			await using var helper = await connectionFactory.CreateConnectionHelper(Network);
			await helper.SaveTransactions(transactions.Select(t => (t, null as uint256, slimBlock?.Hash, null as int?, (long?)slimBlock?.Height, false)), now);
			return transactions.Select(t => new Repository.SavedTransaction()
			{
				BlockHash = slimBlock?.Hash,
				Timestamp = now,
				Transaction = t
			}).ToList();
		}

		public async Task SetIndexProgress(BlockLocator locator)
		{
			await using var conn = await connectionFactory.CreateConnection();
			if (locator is not null)
			{
				await conn.ExecuteAsync(
					"INSERT INTO nbxv1_settings VALUES (@code, 'BlockLocator', @data)" +
					"ON CONFLICT (code, key) DO UPDATE SET data_bytes=EXCLUDED.data_bytes;", new { code = Network.CryptoCode, data = locator.ToBytes() });
			}
			else
			{
				await conn.ExecuteAsync("DELETE FROM nbxv1_settings WHERE code=@code AND key='BlockLocator';", new { code = Network.CryptoCode });
			}
		}
		public async Task<BlockLocator> GetIndexProgress()
		{
			await using var connection = await connectionFactory.CreateConnection();
			var data = await connection.QueryFirstOrDefaultAsync<byte[]>("SELECT data_bytes FROM nbxv1_settings WHERE code=@code AND key='BlockLocator'", new { code = Network.CryptoCode });
			if (data is null)
				return null;
			var locator = new BlockLocator();
			locator.ReadWrite(data, Network.NBitcoinNetwork);
			return locator;
		}

		public async Task Track(IDestination address)
		{
			await using var conn = await GetConnection();
			var walletKey = GetWalletKey(address);
			await conn.Connection.ExecuteAsync(
				"INSERT INTO wallets VALUES (@wid, @metadata::JSONB) ON CONFLICT DO NOTHING;" +
				"INSERT INTO scripts VALUES (@code, @script, @addr) ON CONFLICT DO NOTHING;" +
				"INSERT INTO wallets_scripts VALUES (@code, @script, @wid) ON CONFLICT DO NOTHING"
				, new { code = Network.CryptoCode, script = address.ScriptPubKey.ToHex(), addr = address.ScriptPubKey.GetDestinationAddress(Network.NBitcoinNetwork).ToString(), walletKey.wid, walletKey.metadata });
		}

		public async ValueTask<int> TrimmingEvents(int maxEvents, CancellationToken cancellationToken = default)
		{
			await using var conn = await GetConnection();
			var id = conn.Connection.ExecuteScalar<long?>("SELECT id FROM nbxv1_evts WHERE code=@code ORDER BY id DESC OFFSET @maxEvents LIMIT 1", new { code = Network.CryptoCode, maxEvents = maxEvents - 1 });
			if (id is long i)
				return await conn.Connection.ExecuteAsync("DELETE FROM nbxv1_evts WHERE code=@code AND id < @id", new { code = Network.CryptoCode, id = i });
			return 0;
		}

		private Task<DbConnectionHelper> GetConnection()
		{
			return connectionFactory.CreateConnectionHelper(Network);
		}

		public async Task UpdateAddressPool(DerivationSchemeTrackedSource trackedSource, Dictionary<DerivationFeature, int?> highestKeyIndexFound)
		{
			await using var conn = await GetConnection();

			var parameters = KeyPathTemplates
				.GetSupportedDerivationFeatures()
				.Select(p =>
				{
					if (highestKeyIndexFound.TryGetValue(p, out var highest) && highest is int h)
						return new { DerivationFeature = p, HighestKeyIndexFound = h };
					return null;
				})
				.Where(p => p is not null)
				.Select(p => new
				{
					code = Network.CryptoCode,
					descriptor = this.GetDescriptorKey(trackedSource.DerivationStrategy, p.DerivationFeature).descriptor,
					next_index = p.HighestKeyIndexFound + 1
				})
				.ToArray();
			await conn.Connection.ExecuteAsync("UPDATE descriptors SET next_idx=@next_index WHERE code=@code AND descriptor=@descriptor", parameters);
			await conn.Connection.ExecuteAsync("UPDATE descriptors_scripts SET used='t' WHERE code=@code AND descriptor=@descriptor AND idx < @next_index", parameters);

			foreach (var p in highestKeyIndexFound.Where(k => k.Value is not null))
				await GenerateAddresses(trackedSource.DerivationStrategy, p.Key);
		}

		public async Task NewBlock(SlimChainedBlock newTip)
		{
			await using var conn = await GetConnection();
			var tip = await conn.GetTip();
			if (tip is not null && newTip.Previous != tip.Hash)
				await conn.Connection.ExecuteAsync("UPDATE blks SET confirmed='f' WHERE code=@code AND height >= @height;", new { code = Network.CryptoCode, height = newTip.Height });
			var parameters = new
			{
				code = Network.CryptoCode,
				id = newTip.Hash.ToString(),
				prev = newTip.Previous.ToString(),
				height = newTip.Height
			};
			await conn.Connection.ExecuteAsync(
				"INSERT INTO blks VALUES (@code, @id, @height, @prev) ON CONFLICT DO NOTHING;", parameters);
		}

		public async Task NewBlockCommit(uint256 blockHash)
		{
			await using var conn = await GetConnection();
			await conn.Connection.ExecuteAsync("UPDATE blks SET confirmed='t' WHERE blk_id=@blk_id AND confirmed IS FALSE;",
				new
				{
					code = Network.CryptoCode,
					blk_id = blockHash.ToString()
				});
		}
	}

	public class LegacyDescriptorMetadata
	{
		public const string TypeName = "NBXv1-Derivation";
		[JsonProperty]
		public string Type { get; set; }
		[JsonProperty]
		public DerivationStrategyBase Derivation { get; set; }
		[JsonProperty]
		public KeyPathTemplate KeyPathTemplate { get; set; }
		[JsonConverter(typeof(StringEnumConverter))]
		public DerivationFeature Feature { get; set; }
	}
}
