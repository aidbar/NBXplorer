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

namespace NBXplorer
{
	public class RepositoryProviderLegacy : IRepositoryProvider
	{
		Dictionary<string, RepositoryLegacy> _Repositories = new Dictionary<string, RepositoryLegacy>();
		ExplorerConfiguration _Configuration;
		public Task StartCompletion => Task.CompletedTask;

		public NBXplorerNetworkProvider Networks { get; }
		public DbConnectionFactory ConnectionFactory { get; }

		public RepositoryProviderLegacy(NBXplorerNetworkProvider networks,
			ExplorerConfiguration configuration,
			DbConnectionFactory connectionFactory)
		{
			Networks = networks;
			_Configuration = configuration;
			ConnectionFactory = connectionFactory;
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

		public Task StartAsync(CancellationToken cancellationToken)
		{
			foreach (var net in Networks.GetAll())
			{
				var settings = GetChainSetting(net);
				if (settings != null)
				{
					var repo = net.NBitcoinNetwork.NetworkSet == Liquid.Instance ? throw new NotSupportedException() : new RepositoryLegacy(ConnectionFactory, net);
					repo.MaxPoolSize = _Configuration.MaxGapSize;
					repo.MinPoolSize = _Configuration.MinGapSize;
					repo.MinUtxoValue = settings.MinUtxoValue;
					_Repositories.Add(net.CryptoCode, repo);
				}
			}
			return Task.CompletedTask;
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
		public RepositoryLegacy(DbConnectionFactory connectionFactory, NBXplorerNetwork network)
		{
			this.connectionFactory = connectionFactory;
			Network = network;
			Serializer = new Serializer(network);
		}

		public int BatchSize { get; set; }
		public int MaxPoolSize { get; set; }
		public int MinPoolSize { get; set; }
		public Money MinUtxoValue { get; set; }

		public NBXplorerNetwork Network { get; set; }

		public Serializer Serializer { get; set; }

		public Task CancelReservation(DerivationStrategyBase strategy, KeyPath[] keyPaths)
		{
			throw new NotImplementedException();
		}

		public TrackedTransaction CreateTrackedTransaction(TrackedSource trackedSource, ITrackedTransactionSerializable tx)
		{
			throw new NotImplementedException();
		}

		public TrackedTransaction CreateTrackedTransaction(TrackedSource trackedSource, TrackedTransactionKey transactionKey, IEnumerable<Coin> coins, Dictionary<Script, KeyPath> knownScriptMapping)
		{
			throw new NotImplementedException();
		}

		public TrackedTransaction CreateTrackedTransaction(TrackedSource trackedSource, TrackedTransactionKey transactionKey, Transaction tx, Dictionary<Script, KeyPath> knownScriptMapping)
		{
			throw new NotImplementedException();
		}

		public ValueTask<int> DefragmentTables(CancellationToken cancellationToken = default)
		{
			return default;
		}

		public async Task<int> GenerateAddresses(DerivationStrategyBase strategy, DerivationFeature derivationFeature, GenerateAddressQuery query = null)
		{
			await using var connection = await connectionFactory.CreateConnectionHelper(Network);
			var wid = new DerivationSchemeTrackedSource(strategy).GetLegacyWalletId(Network);
			return await connection.GenerateAddresses(wid, wid, derivationFeature.ToString(), query);
		}

		public Task<int> GenerateAddresses(DerivationStrategyBase strategy, DerivationFeature derivationFeature, int maxAddresses)
		{
			return GenerateAddresses(strategy, derivationFeature, new GenerateAddressQuery(null, maxAddresses));
		}

		public async Task<IList<NewEventBase>> GetEvents(long lastEventId, int? limit = null)
		{
			await using var connection = await connectionFactory.CreateConnection();
			var limitClause = string.Empty;
			if (limit is int i)
				limitClause = $" LIMIT {i}";
			var res = (await connection.QueryAsync<(long id, string data)>($"SELECT id, data FROM evts WHERE code=@code{limitClause}", new { code = Network.CryptoCode }))
				.Select(r =>
				{
					var ev = NewEventBase.ParseEvent(r.data, Serializer.Settings);
					ev.EventId = r.id;
					return ev;
				})
				.ToArray();
			return res;
		}

		public async Task<BlockLocator> GetIndexProgress()
		{
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

		public Task<MultiValueDictionary<Script, KeyPathInformation>> GetKeyInformations(IList<Script> scripts)
		{
			throw new NotImplementedException();
		}

		public Task<IList<NewEventBase>> GetLatestEvents(int limit = 10)
		{
			throw new NotImplementedException();
		}

		FixedSizeCache<uint256, uint256> noMatchCache = new FixedSizeCache<uint256, uint256>(5000, k => k);

		record ScriptPubKeyQuery(string code, string id);

		record SpentUTXORow(System.String code, System.String wallet_id, System.String spent_outpoint, System.String scriptpubkey, System.String keypath_template, System.Int32 idx);
		record ReceivedUTXORow(System.String code, System.String wallet_id, System.String scriptpubkey, System.String keypath);

		public async Task<TrackedTransaction[]> GetMatches(IList<Transaction> txs, uint256 blockId, DateTimeOffset now, bool useCache)
		{
			List<string> outputQuery = new List<string>();
			List<string> scriptPubKeyQuery = new List<string>();
			var txPerSpentOutpoints = new Dictionary<string, Transaction>();
			var trackedPerKey = new Dictionary<(TrackedTransactionKey TrackedTransactionKey, string WalletId), TrackedTransaction>();
			var txPerReceivedScriptPubKey = new MultiValueDictionary<Script, Transaction>();
			foreach (var tx in txs)
			{
				tx.PrecomputeHash(false, true);
				if (blockId != null && useCache && noMatchCache.Contains(tx.GetHash()))
					continue;
				if (!tx.IsCoinBase)
				{
					int i = 0;
					foreach (var input in tx.Inputs)
					{
						var outpoint = input.PrevOut.ToString();
						outputQuery.Add(outpoint);
						txPerSpentOutpoints.Add(outpoint, tx);
						i++;
					}
				}
				foreach (var output in tx.Outputs)
				{
					scriptPubKeyQuery.Add(output.ScriptPubKey.ToHex());
					txPerReceivedScriptPubKey.Add(output.ScriptPubKey, tx);
				}
			}

			await using var connection = await connectionFactory.CreateConnection();
			var spentUtxos = await connection.QueryAsync<SpentUTXORow>(
				"SELECT * FROM tracked_outs " +
				"WHERE code = @code AND spent_outpoint = ANY(@outputQuery)", new { code = Network.CryptoCode, outputQuery = outputQuery });
			var receivedUtxos = await connection.QueryAsync<ReceivedUTXORow>(
				"SELECT code, wallet_id, scriptpubkey, keypath FROM tracked_scriptpubkeys " +
				"WHERE code = @code AND scriptpubkey = ANY(@scriptPubKeyQuery)", new { code = Network.CryptoCode, scriptPubKeyQuery });

			foreach (var spent in spentUtxos)
			{
				var tx = txPerSpentOutpoints[spent.spent_outpoint];
				var trackedTransaction = GetTrackedTransaction(trackedPerKey, spent.wallet_id, blockId, tx);
				trackedTransaction.KnownKeyPathMapping.TryAdd(new Script(Encoders.Hex.DecodeData(spent.scriptpubkey)),
					KeyPathTemplate.Parse(spent.keypath_template).GetKeyPath(spent.idx, false));
			}
			foreach (var received in receivedUtxos)
			{
				var scriptpubkey = Script.FromBytesUnsafe(Encoders.Hex.DecodeData(received.scriptpubkey));
				foreach (var tx in txPerReceivedScriptPubKey[scriptpubkey])
				{
					var trackedTransaction = GetTrackedTransaction(trackedPerKey, received.wallet_id, blockId, tx);
					if (received.keypath != null)
						trackedTransaction.KnownKeyPathMapping.TryAdd(scriptpubkey, KeyPath.Parse(received.keypath));
				}
			}
			foreach (var kv in trackedPerKey)
				kv.Value.KnownKeyPathMappingUpdated();
			return trackedPerKey.Values.ToArray();
		}

		private TrackedTransaction GetTrackedTransaction(Dictionary<(TrackedTransactionKey TrackedTransactionKey, string WalletId), TrackedTransaction> trackedPerKey, string walletId, uint256 blockId, Transaction tx)
		{
			var key = new TrackedTransactionKey(tx.GetHash(), blockId, false);
			if (!trackedPerKey.TryGetValue((key, walletId), out var trackedTransaction))
			{
				trackedTransaction = new TrackedTransaction(key, GetTrackedSource(walletId), tx, new Dictionary<Script, KeyPath>());
				trackedPerKey.Add((key, walletId), trackedTransaction);
			}

			return trackedTransaction;
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

		public Task<TMetadata> GetMetadata<TMetadata>(TrackedSource source, string key) where TMetadata : class
		{
			throw new NotImplementedException();
		}

		public Task<Dictionary<OutPoint, TxOut>> GetOutPointToTxOut(IList<OutPoint> outPoints)
		{
			throw new NotImplementedException();
		}

		public Task<Repository.SavedTransaction[]> GetSavedTransactions(uint256 txid)
		{
			throw new NotImplementedException();
		}

		public Task<TrackedTransaction[]> GetTransactions(TrackedSource trackedSource, uint256 txId = null, CancellationToken cancellation = default)
		{
			throw new NotImplementedException();
		}

		public Task<KeyPathInformation> GetUnused(DerivationStrategyBase strategy, DerivationFeature derivationFeature, int n, bool reserve)
		{
			throw new NotImplementedException();
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

		public Task Prune(TrackedSource trackedSource, IEnumerable<TrackedTransaction> prunable)
		{
			throw new NotImplementedException();
		}

		public async Task<long> SaveEvent(NewEventBase evt)
		{
			await using var helper = await GetConnection();
			var json = evt.ToJObject(Serializer.Settings).ToString();
			var id = helper.Connection.ExecuteScalar<long>("INSERT INTO evts (code, type, data) VALUES (@code, @type, @data::json) RETURNING id", new { code = Network.CryptoCode, type = evt.EventType, data = json });
			return id;
		}

		public Task SaveKeyInformations(KeyPathInformation[] keyPathInformations)
		{
			throw new NotImplementedException();
		}

		public async Task SaveMatches(TrackedTransaction[] transactions)
		{
			if (transactions.Length is 0)
				return;
			await using var helper = await connectionFactory.CreateConnectionHelper(Network);
			var connection = helper.Connection;
			foreach (var tx in transactions)
			{
				await helper.SaveTransactions(new[] { tx.Transaction }, tx.BlockHash, null);
				var outs = tx.GetReceivedOutputs()
					.Select(received =>
					new {
						code = Network.CryptoCode,
						tx_id = tx.TransactionHash.ToString(),
						idx = received.Index,
						scriptpubkey = received.ScriptPubKey.ToHex(),
						val = ((Money)received.Value).Satoshi
					}).ToArray();
				var wallets_outs = tx.GetReceivedOutputs()
					.Select(received => new
					{
						wallet_id = tx.TrackedSource.GetLegacyWalletId(Network),
						code = Network.CryptoCode,
						tx_id = tx.TransactionHash.ToString(),
						idx = received.Index
					}).ToArray();
				await connection.ExecuteAsync("INSERT INTO outs VALUES (@code, @tx_id, @idx, @scriptpubkey, @val) ON CONFLICT DO NOTHING", outs);
				await connection.ExecuteAsync("INSERT INTO wallets_outs VALUES (@wallet_id, @code, @tx_id, @idx) ON CONFLICT DO NOTHING", wallets_outs);
				var ins = tx.SpentOutpoints
					.Select(spent => new
					{
						code = Network.CryptoCode,
						input_tx_id = tx.TransactionHash.ToString(),
						spent_tx_id = spent.Hash.ToString(),
						spent_idx = (int)spent.N
					})
					.ToArray();
				await connection.ExecuteAsync(
					"INSERT INTO ins " +
					"SELECT i.* FROM (VALUES (@code, @input_tx_id, @spent_tx_id, @spent_idx)) i " +
					"WHERE EXISTS (SELECT FROM outs WHERE code = @code AND tx_id = @spent_tx_id AND idx = @spent_idx) FOR SHARE " +
					"ON CONFLICT DO NOTHING", ins);
			}
		}

		public Task SaveMetadata<TMetadata>(TrackedSource source, string key, TMetadata value) where TMetadata : class
		{
			throw new NotImplementedException();
		}

		public async Task<List<Repository.SavedTransaction>> SaveTransactions(DateTimeOffset now, Transaction[] transactions, uint256 blockHash)
		{
			await using var helper = await connectionFactory.CreateConnectionHelper(Network);
			await helper.SaveTransactions(transactions, blockHash, now);
			return transactions.Select(t => new Repository.SavedTransaction()
			{
				BlockHash = blockHash,
				Timestamp = now,
				Transaction = t
			}).ToList();
		}

		public Task SetIndexProgress(BlockLocator locator)
		{
			throw new NotImplementedException();
		}

		public Task Track(IDestination address)
		{
			throw new NotImplementedException();
		}

		public ValueTask<int> TrimmingEvents(int maxEvents, CancellationToken cancellationToken = default)
		{
			return default;
		}

		private Task<DbConnectionHelper> GetConnection()
		{
			return connectionFactory.CreateConnectionHelper(Network);
		}

		public Task UpdateAddressPool(DerivationSchemeTrackedSource trackedSource, Dictionary<DerivationFeature, int?> highestKeyIndexFound)
		{
			throw new NotImplementedException();
		}
	}
}
