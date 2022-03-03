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

		public Task StartAsync(CancellationToken cancellationToken)
		{
			foreach (var net in Networks.GetAll())
			{
				var settings = GetChainSetting(net);
				if (settings != null)
				{
					var repo = net.NBitcoinNetwork.NetworkSet == Liquid.Instance ? throw new NotSupportedException() : new RepositoryLegacy(ConnectionFactory, net, KeyPathTemplates);
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
		public DbConnectionFactory ConnectionFactory => connectionFactory;
		public RepositoryLegacy(DbConnectionFactory connectionFactory, NBXplorerNetwork network, KeyPathTemplates keyPathTemplates)
		{
			this.connectionFactory = connectionFactory;
			Network = network;
			KeyPathTemplates = keyPathTemplates;
			Serializer = new Serializer(network);
		}

		public int BatchSize { get; set; }
		public int MaxPoolSize { get; set; }
		public int MinPoolSize { get; set; }
		public Money MinUtxoValue { get; set; }

		public NBXplorerNetwork Network { get; set; }
		public KeyPathTemplates KeyPathTemplates { get; }
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
			foreach (var kv in await connection.GetUTXOs(outpoints))
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


		public async Task<Dictionary<OutPoint, TxOut>> GetOutPointToTxOut(IList<OutPoint> outPoints)
		{
			await using var connection = await connectionFactory.CreateConnectionHelper(Network);
			return await connection.GetUTXOs(outPoints);
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

		public async Task<TrackedTransaction[]> GetTransactions(TrackedSource trackedSource, uint256 txId = null, CancellationToken cancellation = default)
		{
			await using var connection = await connectionFactory.CreateConnectionHelper(Network);
			var tip = await connection.GetTip();
			if (tip is null)
				return Array.Empty<TrackedTransaction>();
			var utxos = await 
				connection.Connection.QueryAsync<(string tx_id, byte[] raw, string block_id, int idx, string script, long value, DateTime seen_at)>(
				"SELECT t.tx_id, t.raw, t.blk_id, o.idx, o.script, o.value, t.seen_at " +
				"FROM tracked_scripts ts " +
				"JOIN outs o USING (code, script) " +
				"JOIN txs t ON t.code = o.code AND t.tx_id = o.tx_id " +
				"WHERE ts.code=@code AND ts.wallet_id=@walletId", new { code = Network.CryptoCode, walletId = trackedSource.GetLegacyWalletId(Network) });
			utxos.TryGetNonEnumeratedCount(out int c);
			var trackedById = new Dictionary<string, TrackedTransaction>(c);
			foreach (var utxo in utxos)
			{
				var tracked = GetTrackedTransaction(trackedSource, utxo.tx_id, utxo.raw, utxo.block_id, trackedById);
				var txout = Network.NBitcoinNetwork.Consensus.ConsensusFactory.CreateTxOut();
				txout.Value = Money.Satoshis(utxo.value);
				txout.ScriptPubKey = Script.FromHex(utxo.script);
				tracked.ReceivedCoins.Add(new Coin(new OutPoint(tracked.Key.TxId, utxo.idx), txout));
			}
			return trackedById.Values.ToArray();
		}

		private TrackedTransaction GetTrackedTransaction(TrackedSource trackedSource, string tx_id, byte[] raw, string block_id, Dictionary<string, TrackedTransaction> trackedById)
		{
			if (trackedById.TryGetValue(tx_id, out var tracked))
				return tracked;
			TrackedTransactionKey key;
			bool hasTx = raw is not null;
			if (block_id is not null)
			{
				key = new TrackedTransactionKey(uint256.Parse(tx_id), uint256.Parse(block_id), !hasTx);
			}
			else
			{
				key = new TrackedTransactionKey(uint256.Parse(tx_id), null, !hasTx);
			}
			if (hasTx)
				tracked = CreateTrackedTransaction(trackedSource, key, Transaction.Load(raw, Network.NBitcoinNetwork), new Dictionary<Script, KeyPath>());
			else
				tracked = CreateTrackedTransaction(trackedSource, key, null as IEnumerable<Coin>, new Dictionary<Script, KeyPath>());
			trackedById.Add(tx_id, tracked);
			return tracked;
		}

		public async Task<KeyPathInformation> GetUnused(DerivationStrategyBase strategy, DerivationFeature derivationFeature, int n, bool reserve)
		{
			await using var connection = await connectionFactory.CreateConnectionHelper(Network);
			return await connection.GetUnused(new LegacyDescriptor(strategy, KeyPathTemplates.GetKeyPathTemplate(derivationFeature)), n, reserve);
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
				"INSERT INTO scripts_wallets VALUES (@code, @script, @walletId) ON CONFLICT DO NOTHING"
				, new { code = Network.CryptoCode, script = address.ScriptPubKey.ToHex(), addr = address.ScriptPubKey.GetDestinationAddress(Network.NBitcoinNetwork).ToString(), walletId });
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

		public async Task NewBlock(SlimChainedBlock newTip)
		{
			await using var conn = await GetConnection();
			var tip = await conn.GetTip();
			if (tip is not null && newTip.Previous != tip.Hash)
				await conn.Connection.ExecuteScalarAsync<int>("UPDATE blks SET confirmed='f' WHERE height >= @newtipheight", new { newtipheight = newTip.Height });
			var parameters = new
			{
				code = Network.CryptoCode,
				id = newTip.Hash.ToString(),
				prev = newTip.Previous.ToString(),
				height = newTip.Height,
				maturity = newTip.Height - Network.NBitcoinNetwork.Consensus.CoinbaseMaturity + 1
			};
			await conn.Connection.ExecuteAsync(
				"INSERT INTO blks VALUES (@code, @id, @height, @prev) ON CONFLICT (code, blk_id) DO UPDATE SET confirmed='t';" +
				"SELECT set_maturity_below_height(@code, @maturity);", parameters);
		}
	}
}
