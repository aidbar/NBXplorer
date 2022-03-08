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

namespace NBXplorer
{
	public class RepositoryProviderLegacy : IRepositoryProvider
	{
		Dictionary<string, RepositoryLegacy> _Repositories = new Dictionary<string, RepositoryLegacy>();
		ExplorerConfiguration _Configuration;

		public Task StartCompletion => Task.CompletedTask;

		public NBXplorerNetworkProvider Networks { get; }
		public DbConnectionFactory ConnectionFactory { get; }
		public KeyPathTemplates KeyPathTemplates { get; }

		public RepositoryProviderLegacy(NBXplorerNetworkProvider networks,
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
			_Repositories.TryGetValue(cryptoCode, out RepositoryLegacy repository);
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
						new RepositoryLegacy(ConnectionFactory, net, KeyPathTemplates, settings.RPC);
					repo.MaxPoolSize = _Configuration.MaxGapSize;
					repo.MinPoolSize = _Configuration.MinGapSize;
					repo.MinUtxoValue = settings.MinUtxoValue;
					_Repositories.Add(net.CryptoCode, repo);
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
	public class RepositoryLegacy : IRepository
	{
		private DbConnectionFactory connectionFactory;
		private readonly RPCClient rpc;

		public DbConnectionFactory ConnectionFactory => connectionFactory;
		public RepositoryLegacy(DbConnectionFactory connectionFactory, NBXplorerNetwork network, KeyPathTemplates keyPathTemplates, RPCClient rpc)
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
					var descriptor = new LegacyDescriptor(strategy, KeyPathTemplates.GetKeyPathTemplate(o));
					return new
					{
						code = Network.CryptoCode,
						descriptor = descriptor.ToString(),
						idx = (int)descriptor.KeyPathTemplate.GetIndex(o)
					};
				})
				.ToList();
			await conn.Connection.ExecuteAsync(
				"WITH cte AS (SELECT code, script FROM descriptors_scripts WHERE code=@code AND descriptor=@descriptor AND idx=@idx) " +
				"UPDATE scripts s SET used='f' FROM cte WHERE s.code=cte.code AND s.script=cte.script", parameters);
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

		public async Task<int> GenerateAddresses(DerivationStrategyBase strategy, DerivationFeature derivationFeature, GenerateAddressQuery query = null)
		{
			await using var connection = await connectionFactory.CreateConnectionHelper(Network);
			return await connection.GenerateAddresses(new LegacyDescriptor(strategy, KeyPathTemplates.GetKeyPathTemplate(derivationFeature)), query);
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
			var res = (await connection.QueryAsync<(long id, string data)>($"SELECT id, data FROM evts WHERE code=@code AND id > @lastEventId ORDER BY id{limitClause}", new { code = Network.CryptoCode, lastEventId }))
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
			var res = (await connection.QueryAsync<(long id, string data)>($"SELECT id, data FROM evts WHERE code=@code ORDER BY id DESC{limitClause}", new { code = Network.CryptoCode }))
				.Select(ToTypedEvent)
				.ToArray();
			Array.Reverse(res);
			return res;
		}

		public async Task<BlockLocator> GetIndexProgress()
		{
			// TODO: WE SHOULD NOT RELY ON THE BLKS TABLE FOR STORING THE INDEX PROGRESS
			await using var connection = await connectionFactory.CreateConnection();
			var blocks = (await connection.QueryAsync<string>("SELECT blk_id FROM blks WHERE code=@code AND confirmed='t' ORDER BY height DESC LIMIT 1000;", new { code = Network.CryptoCode }))
				.Select(b => uint256.Parse(b))
				.ToArray();
			if (blocks.Length is 0)
				return null;
			var locators = new BlockLocator();
			locators.Blocks = new List<uint256>(blocks);
			return locators;
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
			foreach (var row in await connection.QueryAsync<(string script, string addr, string source, string descriptor, string keypath)>(
				"SELECT script, addr, source, descriptor, keypath FROM tracked_scripts WHERE code=@code AND script=ANY(@scripts)", new { code = Network.CryptoCode, scripts = scripts.Select(s => s.ToHex()).ToList() }))
			{

				bool isDescriptor = row.source == "DESCRIPTOR";
				bool isExplicit = row.source == "EXPLICIT";
				var descriptor = isDescriptor ? Descriptor.Parse(row.descriptor, Network) : null;
				var script = Script.FromHex(row.script);
				var derivationStrategy = (descriptor as LegacyDescriptor)?.DerivationStrategy;
				var addr = BitcoinAddress.Create(row.addr, Network.NBitcoinNetwork);
				var keypath = isDescriptor ? KeyPath.Parse(row.keypath) : null;
				result.Add(script, new KeyPathInformation()
				{
					Address = addr,
					DerivationStrategy = isDescriptor ? derivationStrategy : null,
					KeyPath = isDescriptor ? keypath : null,
					ScriptPubKey = script,
					TrackedSource = isDescriptor && derivationStrategy is not null ? new DerivationSchemeTrackedSource(derivationStrategy) :
									isExplicit ? new AddressTrackedSource(addr) : null,
					Feature = keypath is null ? DerivationFeature.Deposit : KeyPathTemplates.GetDerivationFeature(keypath),
					// TODO: Redeem = 
				});
			}
			return result;
		}

		FixedSizeCache<uint256, uint256> noMatchCache = new FixedSizeCache<uint256, uint256>(5000, k => k);

		record ScriptPubKeyQuery(string code, string id);

		public Task<TrackedTransaction[]> GetMatches(Block block, uint256 blockId, DateTimeOffset now, bool useCache)
		{
			return GetMatches(block.Transactions, blockId, now, useCache);
		}

		public async Task<TrackedTransaction[]> GetMatches(IList<Transaction> txs, uint256 blockId, DateTimeOffset now, bool useCache)
		{
			foreach (var tx in txs)
				tx.PrecomputeHash(false, true);

			var outputCount = txs.Select(tx => tx.Outputs.Count).Sum();
			var inputCount = txs.Select(tx => tx.Inputs.Count).Sum();
			var outpointCount = inputCount + outputCount;

			var scripts = new List<Script>(outpointCount);
			var transactionsPerOutpoint = new MultiValueDictionary<OutPoint, NBitcoin.Transaction>(inputCount);
			var transactionsPerScript = new MultiValueDictionary<Script, NBitcoin.Transaction>(outpointCount);

			var matches = new Dictionary<string, TrackedTransaction>();
			var noMatchTransactions = new HashSet<uint256>(txs.Count);
			var transactions = new Dictionary<uint256, NBitcoin.Transaction>(txs.Count);
			var outpoints = new List<OutPoint>(inputCount);
			foreach (var tx in txs)
			{
				if (!transactions.TryAdd(tx.GetHash(), tx))
					continue;
				if (blockId != null && useCache && noMatchCache.Contains(tx.GetHash()))
				{
					continue;
				}
				noMatchTransactions.Add(tx.GetHash());
				if (!tx.IsCoinBase)
				{
					foreach (var input in tx.Inputs)
					{
						transactionsPerOutpoint.Add(input.PrevOut, tx);
						if (transactions.TryGetValue(input.PrevOut.Hash, out var prevtx))
						{
							// Maybe this tx is spending another tx in the same block, in which case, it will not be fetched by GetOutPointToTxOut,
							// so we need to add it here.
							var txout = prevtx.Outputs[input.PrevOut.N];
							scripts.Add(txout.ScriptPubKey);
							transactionsPerScript.Add(txout.ScriptPubKey, tx);
						}
						else
						{
							// Else, let's try to fetch it later.
							outpoints.Add(input.PrevOut);
						}
					}
				}
				foreach (var output in tx.Outputs)
				{
					if (MinUtxoValue != null && output.Value < MinUtxoValue)
						continue;
					scripts.Add(output.ScriptPubKey);
					transactionsPerScript.Add(output.ScriptPubKey, tx);
				}
			}

			await using var connection = await connectionFactory.CreateConnectionHelper(Network);
			foreach (var kv in await connection.GetOutputs(outpoints))
			{
				if (kv.Value is null)
					continue;
				scripts.Add(kv.Value.ScriptPubKey);
				foreach (var tx in transactionsPerOutpoint[kv.Key])
				{
					transactionsPerScript.Add(kv.Value.ScriptPubKey, tx);
				}
			}
			if (scripts.Count == 0)
				return Array.Empty<TrackedTransaction>();
			var keyPathInformationsByTrackedTransaction = new MultiValueDictionary<TrackedTransaction, KeyPathInformation>();
			var keyInformations = await GetKeyInformations(connection.Connection, scripts);
			foreach (var keyInfoByScripts in keyInformations)
			{
				foreach (var tx in transactionsPerScript[keyInfoByScripts.Key])
				{
					if (keyInfoByScripts.Value.Count != 0)
						noMatchTransactions.Remove(tx.GetHash());
					foreach (var keyInfo in keyInfoByScripts.Value)
					{
						var matchesGroupingKey = $"{keyInfo.DerivationStrategy?.ToString() ?? keyInfo.ScriptPubKey.ToHex()}-[{tx.GetHash()}]";
						if (!matches.TryGetValue(matchesGroupingKey, out TrackedTransaction match))
						{
							match = CreateTrackedTransaction(keyInfo.TrackedSource,
								new TrackedTransactionKey(tx.GetHash(), blockId, false),
								tx,
								new Dictionary<Script, KeyPath>());
							match.FirstSeen = now;
							match.Inserted = now;
							matches.Add(matchesGroupingKey, match);
						}
						if (keyInfo.KeyPath != null)
							match.KnownKeyPathMapping.TryAdd(keyInfo.ScriptPubKey, keyInfo.KeyPath);
						keyPathInformationsByTrackedTransaction.Add(match, keyInfo);
					}
				}
			}
			foreach (var m in matches.Values)
			{
				m.KnownKeyPathMappingUpdated();
			}

			foreach (var tx in txs)
			{
				if (blockId == null &&
					noMatchTransactions.Contains(tx.GetHash()))
				{
					noMatchCache.Add(tx.GetHash());
				}
			}
			return matches.Values.Count == 0 ? Array.Empty<TrackedTransaction>() : matches.Values.ToArray();
		}

		private TrackedSource GetTrackedSource(string wallet_id)
		{
			var legacyHeader = $"Legacy({Network.CryptoCode}):";
			var legacyScheme = wallet_id;
			if (wallet_id.StartsWith(legacyHeader, StringComparison.OrdinalIgnoreCase))
				legacyScheme = wallet_id.Substring(legacyHeader.Length);
			return TrackedSource.Parse(legacyScheme, Network);
		}

		public Task<TrackedTransaction[]> GetMatches(Transaction tx, uint256 blockId, DateTimeOffset now, bool useCache)
		{
			return GetMatches(new[] { tx }, blockId, now, useCache);
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
				connection.Connection.QueryAsync<(string tx_id, string block_id, string source, string out_tx_id, int idx, string script, long value, bool immature, string keypath, DateTime seen_at)>(
				"SELECT io.tx_id, io.blk_id, io.source, out_tx_id, io.idx, io.script, io.value, io.immature, ts.keypath, io.seen_at " +
				"FROM tracked_scripts ts " +
				"JOIN ins_outs io USING (code, script) " +
				"WHERE ts.code=@code AND ts.wallet_id=@walletId", new { code = Network.CryptoCode, walletId = trackedSource.GetLegacyWalletId(Network) });
			utxos.TryGetNonEnumeratedCount(out int c);
			var trackedById = new Dictionary<string, TrackedTransaction>(c);
			var txIdStr = txId?.ToString();
			foreach (var utxo in utxos)
			{
				if (txIdStr != null && utxo.tx_id != txIdStr)
					continue;
				var tracked = GetTrackedTransaction(trackedSource, utxo.tx_id, utxo.block_id, utxo.seen_at, trackedById);
				if (utxo.source == "OUTPUT")
				{
					var txout = Network.NBitcoinNetwork.Consensus.ConsensusFactory.CreateTxOut();
					txout.Value = Money.Satoshis(utxo.value);
					txout.ScriptPubKey = Script.FromHex(utxo.script);
					tracked.ReceivedCoins.Add(new Coin(new OutPoint(tracked.Key.TxId, utxo.idx), txout));
					tracked.IsCoinBase = utxo.immature;
					if (utxo.keypath is string)
						tracked.KnownKeyPathMapping.Add(txout.ScriptPubKey, KeyPath.Parse(utxo.keypath));
				}
				else
				{
					tracked.SpentOutpoints.Add(new OutPoint(uint256.Parse(utxo.out_tx_id), utxo.idx));
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
			await using var connection = await connectionFactory.CreateConnectionHelper(Network);
			 var keyInfo = await connection.GetUnused(new LegacyDescriptor(strategy, KeyPathTemplates.GetKeyPathTemplate(derivationFeature)), n, reserve);
			if (keyInfo != null)
				await ImportAddressToRPC(connection, keyInfo.TrackedSource, keyInfo.Address, keyInfo.KeyPath);
			return keyInfo;
		}

		public async Task SaveKeyInformations(KeyPathInformation[] keyPathInformations)
		{
			await using var connection = await connectionFactory.CreateConnectionHelper(Network);

			var parameters = keyPathInformations
				.Select(kpi => new
				{
					code = Network.CryptoCode,
					descriptor = ToDescriptor(kpi.TrackedSource, KeyPathTemplates.GetKeyPathTemplate(kpi.Feature)).ToString(),
					script = kpi.ScriptPubKey.ToHex(),
					address = kpi.Address.ToString(),
					idx = kpi.GetIndex(),
					keypath = kpi.KeyPath.ToString()
					// TODO REDEEM??
				})
				.ToArray();
			await connection.Connection.ExecuteAsync("INSERT INTO scripts VALUES (@code, @script, @address, 't') ON CONFLICT (code, script) DO UPDATE SET used='t';", parameters);
			await connection.Connection.ExecuteAsync("INSERT INTO descriptors_scripts VALUES (@code, @descriptor, @idx, @script, @keypath) ON CONFLICT DO NOTHING;", parameters);
		}

		private LegacyDescriptor ToDescriptor(TrackedSource trackedSource, KeyPathTemplate keyPathTemplate)
		{
			return new LegacyDescriptor(((DerivationSchemeTrackedSource)trackedSource).DerivationStrategy, keyPathTemplate);
		}

		private async Task ImportAddressToRPC(DbConnectionHelper connection, TrackedSource trackedSource, BitcoinAddress address, KeyPath keyPath)
		{
			var wid = trackedSource.GetLegacyWalletId(Network);
			var shouldImportRPC = (await connection.GetMetadata<string>(wid, WellknownMetadataKeys.ImportAddressToRPC)).AsBoolean();
			if (!shouldImportRPC)
				return;
			var accountKey = await connection.GetMetadata<BitcoinExtKey>(wid, WellknownMetadataKeys.AccountHDKey);
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
				.SelectMany(c => c.ReceivedCoins)
				.Select(c => new
				{
					code = Network.CryptoCode,
					txId = c.Outpoint.Hash.ToString(),
					idx = (int)c.Outpoint.N
				}).ToArray();
			var spentCoins =
				prunable
				.SelectMany(c => c.SpentOutpoints)
				.Select(c => new
				{
					code = Network.CryptoCode,
					txId = c.Hash.ToString(),
					idx = (int)c.N
				}).ToArray();
			await helper.Connection.ExecuteAsync("DELETE FROM outs WHERE code=@code AND tx_id=@txId AND idx=@idx", receivedCoinsToDelete);
			await helper.Connection.ExecuteAsync("DELETE FROM ins WHERE code=@code AND spent_tx_id=@txId AND spent_idx=@idx", spentCoins);
		}

		public async Task<long> SaveEvent(NewEventBase evt)
		{
			await using var helper = await GetConnection();
			var json = evt.ToJObject(Serializer.Settings).ToString();
			var id = helper.Connection.ExecuteScalar<long>("INSERT INTO evts (code, type, data) VALUES (@code, @type, @data::json) RETURNING id", new { code = Network.CryptoCode, type = evt.EventType, data = json });
			return id;
		}

		public async Task SaveMatches(TrackedTransaction[] transactions)
		{
			if (transactions.Length is 0)
				return;
			await using var helper = await connectionFactory.CreateConnectionHelper(Network);
			var connection = helper.Connection;
			bool hasBlock = false;
			await helper.SaveTransactions(transactions.Select(t => (t.Transaction, t.TransactionHash, t.BlockHash, t.BlockIndex)), null);
			foreach (var tx in transactions)
			{
				if (tx.BlockHash is not null)
					hasBlock = true;
				var outs = tx.GetReceivedOutputs()
					.Select(received =>
					new
					{
						code = Network.CryptoCode,
						tx_id = tx.TransactionHash.ToString(),
						idx = received.Index,
						scriptpubkey = received.ScriptPubKey.ToHex(),
						val = ((Money)received.Value).Satoshi,
						immature = tx.IsCoinBase
					}).ToArray();
				await connection.ExecuteAsync("INSERT INTO outs VALUES (@code, @tx_id, @idx, @scriptpubkey, @val, @immature) ON CONFLICT DO NOTHING", outs);
				var ins = tx.SpentOutpoints
					.Select(spent => new
					{
						code = Network.CryptoCode,
						input_tx_id = tx.TransactionHash.ToString(),
						input_idx = tx.IndexOfInput(spent),
						spent_tx_id = spent.Hash.ToString(),
						spent_idx = (int)spent.N
					})
					.ToArray();
				await connection.ExecuteAsync(
					"INSERT INTO ins " +
					"SELECT i.* FROM (VALUES (@code, @input_tx_id, @input_idx, @spent_tx_id, @spent_idx)) i " +
					"WHERE EXISTS (SELECT FROM outs WHERE code = @code AND tx_id = @spent_tx_id AND idx = @spent_idx) FOR SHARE " +
					"ON CONFLICT DO NOTHING", ins);
			}
			if (hasBlock)
			{
				await connection.ExecuteAsync("CALL new_block_updated(@code, @maturity)",
					new
					{
						code = Network.CryptoCode,
						maturity = Network.NBitcoinNetwork.Consensus.CoinbaseMaturity
					});
			}
		}

		public async Task SaveMetadata<TMetadata>(TrackedSource source, string key, TMetadata value) where TMetadata : class
		{
			await using var helper = await connectionFactory.CreateConnectionHelper(Network);
			var wid = source.GetLegacyWalletId(Network);
			try
			{
				await helper.SetMetadata(wid, key, value);
			}
			catch (PostgresException ex) when (ex.ConstraintName == "wallet_metadata_wallet_id_fkey")
			{
				await helper.CreateWallet(wid);
				await helper.SetMetadata(wid, key, value);
			}
		}
		public async Task<TMetadata> GetMetadata<TMetadata>(TrackedSource source, string key) where TMetadata : class
		{
			await using var helper = await connectionFactory.CreateConnectionHelper(Network);
			var wid = source.GetLegacyWalletId(Network);
			return await helper.GetMetadata<TMetadata>(wid, key);
		}

		public async Task<List<Repository.SavedTransaction>> SaveTransactions(DateTimeOffset now, Transaction[] transactions, uint256 blockHash)
		{
			await using var helper = await connectionFactory.CreateConnectionHelper(Network);
			await helper.SaveTransactions(transactions.Select(t => (t, null as uint256, blockHash, null as int?)), now);
			return transactions.Select(t => new Repository.SavedTransaction()
			{
				BlockHash = blockHash,
				Timestamp = now,
				Transaction = t
			}).ToList();
		}

		public Task SetIndexProgress(BlockLocator locator)
		{
			// TODO: WE SHOULD NOT RELY ON THE BLKS TABLE FOR STORING THE INDEX PROGRESS
			return Task.CompletedTask;
		}

		public async Task Track(IDestination address)
		{
			await using var conn = await GetConnection();
			var walletId = ((AddressTrackedSource)address).GetLegacyWalletId(Network);
			await conn.Connection.ExecuteAsync(
				"INSERT INTO wallets VALUES (@walletId) ON CONFLICT DO NOTHING;" +
				"INSERT INTO scripts VALUES (@code, @script, @addr) ON CONFLICT DO NOTHING;" +
				"INSERT INTO wallets_scripts VALUES (@code, @script, @walletId) ON CONFLICT DO NOTHING"
				, new { code = Network.CryptoCode, script = address.ScriptPubKey.ToHex(), addr = address.ScriptPubKey.GetDestinationAddress(Network.NBitcoinNetwork).ToString(), walletId });
		}

		public async ValueTask<int> TrimmingEvents(int maxEvents, CancellationToken cancellationToken = default)
		{
			await using var conn = await GetConnection();
			var id = conn.Connection.ExecuteScalar<long?>("SELECT id FROM evts WHERE code=@code ORDER BY id DESC OFFSET @maxEvents LIMIT 1", new { code = Network.CryptoCode, maxEvents = maxEvents - 1 });
			if (id is long i)
				return await conn.Connection.ExecuteAsync("DELETE FROM evts WHERE code=@code AND id < @id", new { code = Network.CryptoCode, id = i });
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
					descriptor = new LegacyDescriptor(trackedSource.DerivationStrategy, KeyPathTemplates.GetKeyPathTemplate(p.DerivationFeature)).ToString(),
					next_index = p.HighestKeyIndexFound + 1
				})
				.ToArray();
			await conn.Connection.ExecuteAsync("UPDATE descriptors SET next_index=@next_index WHERE code=@code AND descriptor=@descriptor", parameters);
			await conn.Connection.ExecuteAsync(
				"WITH cte AS (SELECT code, script FROM descriptors_scripts WHERE code=@code AND descriptor=@descriptor AND idx < @next_index) " +
				"UPDATE scripts s SET used='t' FROM cte WHERE s.code=cte.code AND s.script=cte.script", parameters);

			foreach (var p in highestKeyIndexFound.Where(k => k.Value is not null))
				await GenerateAddresses(trackedSource.DerivationStrategy, p.Key);
		}

		public async Task NewBlock(SlimChainedBlock newTip)
		{
			await using var conn = await GetConnection();
			var tip = await conn.GetTip();
			if (tip is not null && newTip.Previous != tip.Hash)
				await conn.Connection.ExecuteAsync("CALL orphan_blocks(@code, @height);", new { code = Network.CryptoCode, height = newTip.Height });
			var parameters = new
			{
				code = Network.CryptoCode,
				id = newTip.Hash.ToString(),
				prev = newTip.Previous.ToString(),
				height = newTip.Height
			};
			await conn.Connection.ExecuteAsync(
				"INSERT INTO blks VALUES (@code, @id, @height, @prev) ON CONFLICT (code, blk_id) DO UPDATE SET confirmed='t';", parameters);
		}
	}
}
