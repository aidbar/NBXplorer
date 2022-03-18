using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NBitcoin;
using NBXplorer.Altcoins.Liquid;
using NBXplorer.DerivationStrategy;
using NBXplorer.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NBXplorer.HostedServices
{
	public class DBTrieToPostgresMigratorHostedService : IHostedService
	{
		public class MigrationProgress
		{
			public bool EventsMigrated { get; set; }
			public bool KeyPathInformationMigrated { get; set; }
			public bool MetadataMigrated { get; set; }
			public bool HighestPathMigrated { get; set; }
			public bool AvailableKeysMigrated { get; set; }
			public bool SavedTransactionsMigrated { get; set; }
			public bool TrackedTransactionsOutputsMigrated { get; set; }
			public bool TrackedTransactionsInputsMigrated { get; set; }
			public bool BlocksMigrated { get; set; }
		}
		public DBTrieToPostgresMigratorHostedService(
			RepositoryProvider repositoryProvider,
			IRepositoryProvider postgresRepositoryProvider,
			ILogger<DBTrieToPostgresMigratorHostedService> logger,
			IConfiguration configuration,
			KeyPathTemplates keyPathTemplates,
			RPCClientProvider rpcClients)
		{
			LegacyRepositoryProvider = repositoryProvider;
			Logger = logger;
			Configuration = configuration;
			KeyPathTemplates = keyPathTemplates;
			RpcClients = rpcClients;
			PostgresRepositoryProvider = (PostgresRepositoryProvider)postgresRepositoryProvider;
		}

		public RepositoryProvider LegacyRepositoryProvider { get; }
		public ILogger<DBTrieToPostgresMigratorHostedService> Logger { get; }
		public IConfiguration Configuration { get; }
		public KeyPathTemplates KeyPathTemplates { get; }
		public RPCClientProvider RpcClients { get; }
		public PostgresRepositoryProvider PostgresRepositoryProvider { get; }

		bool started;
		public async Task StartAsync(CancellationToken cancellationToken)
		{
			if (!LegacyRepositoryProvider.Exists())
				return;
			var migrationState = LegacyRepositoryProvider.GetMigrationState();
			if (migrationState == RepositoryProvider.MigrationState.Done)
			{
				DeleteAfterMigrationOrWarning();
				return;
			}
			if (migrationState == RepositoryProvider.MigrationState.NotStarted)
			{
				File.WriteAllText(LegacyRepositoryProvider.GetMigrationLockPath(), "InProgress");
			}
			LegacyRepositoryProvider.MigrationMode = true;
			await LegacyRepositoryProvider.StartAsync(cancellationToken);

			foreach (var legacyRepo in LegacyRepositoryProvider.GetRepositories())
			{
				var postgresRepo = PostgresRepositoryProvider.GetRepository(legacyRepo.Network);
				await Migrate(legacyRepo.Network, legacyRepo, (PostgresRepository)postgresRepo);
			}
			started = true;
			if (migrationState == RepositoryProvider.MigrationState.NotStarted)
			{
				File.WriteAllText(LegacyRepositoryProvider.GetMigrationLockPath(), "Done");
				DeleteAfterMigrationOrWarning();
			}
		}

		private void DeleteAfterMigrationOrWarning()
		{
			if (!Configuration.GetValue<bool>("DELETE_AFTER_MIGRATION", false))
				Logger.LogWarning($"A legacy DBTrie database has been previously migrated to postgres and is still present. You can safely delete it if you do not expect using it in the future. To delete the old DBTrie database, start NBXplorer with --delete-after-migration (or environment variable: NBXPLORER_DELETE_AFTER_MIGRATION)");
			else
			{
				Directory.Delete(LegacyRepositoryProvider.GetDatabasePath(), true);
				Logger.LogInformation($"Old migrated legacy DBTrie database has been deleted");
			}
		}

		record InsertDescriptor(string code, string descriptor, string metadata);
		record InsertMetadata(string wallet_id, string key, string value);
		record UpdateNextIndex(string code, string descriptor, long next_idx);
		record UpdateUsedScript(string code, string descriptor, long idx);
		record UpdateBlock(string code, string blk_id, string prev_id, long height);
		record UpdateTransaction(string code, string tx_id, byte[] raw, DateTime seen_at);
		record UpdateBlockTransaction(string code, string tx_id, string blk_id);
		record InsertOuts(string code, string tx_id, long idx, string script, long value, string asset_id);
		record InsertIns(string code, string input_tx_id, long input_idx, string spent_tx_id, long spent_idx);
		private async Task Migrate(NBXplorerNetwork network, Repository legacyRepo, PostgresRepository postgresRepo)
		{
			using var conn = await postgresRepo.ConnectionFactory.CreateConnection();
			var data = await conn.QueryFirstOrDefaultAsync<string>("SELECT data_json FROM nbxv1_settings WHERE code=@code AND key='MigrationProgress'", new { code = network.CryptoCode });
			var progress = data is null ? new MigrationProgress() : JsonConvert.DeserializeObject<MigrationProgress>(data);
			if (!progress.EventsMigrated)
			{
				using (var tx = await conn.BeginTransactionAsync())
				{
					Logger.LogInformation($"{network.CryptoCode}: Migrating events to postgres...");
					long lastEventId = -1;
					nextbatch:
					var batch = await legacyRepo.GetEvents(lastEventId, 100);
					if (batch.Count > 0)
					{
						var parameters = batch.Select(e => new
						{
							code = network.CryptoCode,
							id = e.EventId,
							type = e.EventType,
							data = e.ToJson(network.JsonSerializerSettings)
						}).ToArray();
						await conn.ExecuteAsync("INSERT INTO nbxv1_evts (code, id, type, data) VALUES (@code, @id, @type, @data::json)", parameters);
						lastEventId = parameters.Select(p => p.id).Max();
						await conn.ExecuteAsync("INSERT INTO nbxv1_evts_ids AS ei VALUES (@code, @curr_id) ON CONFLICT (code) DO UPDATE SET curr_id=@curr_id", new { code = network.CryptoCode, curr_id = lastEventId });
						goto nextbatch;
					}

					progress.EventsMigrated = true;
					await SaveProgress(network, conn, progress);
					await tx.CommitAsync();
					Logger.LogInformation($"{network.CryptoCode}: Events migrated.");
				}
			}
			Dictionary<string, TrackedSource> hashToTrackedSource = new Dictionary<string, TrackedSource>();
			if (!progress.KeyPathInformationMigrated)
			{
				Logger.LogInformation($"{network.CryptoCode}: Migrating scripts to postgres...");
				using (var tx = await conn.BeginTransactionAsync())
				{
					List<KeyPathInformation> batch = new List<KeyPathInformation>(100);
					HashSet<PostgresRepository.WalletKey> walletKeys = new HashSet<PostgresRepository.WalletKey>();
					List<InsertDescriptor> descriptors = new List<InsertDescriptor>();
					using var legacyTx = await legacyRepo.engine.OpenTransaction();
					var scriptsTable = legacyTx.GetTable($"{legacyRepo._Suffix}Scripts");
					await foreach (var row in scriptsTable.Enumerate())
					{
						using (row)
						{
							var keyInfo = legacyRepo.ToObject<KeyPathInformation>(await row.ReadValue())
											.AddAddress(network.NBitcoinNetwork);
							hashToTrackedSource.TryAdd(keyInfo.TrackedSource.GetHash().ToString(), keyInfo.TrackedSource);
							batch.Add(keyInfo);
							if (keyInfo.TrackedSource is DerivationSchemeTrackedSource ts)
							{
								var keyTemplate = KeyPathTemplates.GetKeyPathTemplate(keyInfo.Feature);
								var k = postgresRepo.GetDescriptorKey(ts.DerivationStrategy, keyInfo.Feature);
								descriptors.Add(new InsertDescriptor(k.code, k.descriptor, network.Serializer.ToString(new LegacyDescriptorMetadata()
								{
									Derivation = ts.DerivationStrategy,
									Feature = keyInfo.Feature,
									KeyPathTemplate = keyTemplate,
									Type = LegacyDescriptorMetadata.TypeName
								})));
							}
							walletKeys.Add(postgresRepo.GetWalletKey(keyInfo.TrackedSource));
							if (batch.Count == 100)
							{
								await CreateWalletAndDescriptor(conn, walletKeys, descriptors);
								await postgresRepo.SaveKeyInformations(conn, batch.ToArray());
								batch.Clear();
								walletKeys.Clear();
								descriptors.Clear();
							}
						}
					}
					await CreateWalletAndDescriptor(conn, walletKeys, descriptors);
					await postgresRepo.SaveKeyInformations(conn, batch.ToArray());
					Logger.LogInformation($"{network.CryptoCode}: Scripts migrated.");
					progress.KeyPathInformationMigrated = true;
					await SaveProgress(network, conn, progress);
					await tx.CommitAsync();
				}
			}

			if (hashToTrackedSource.Count is 0)
			{
				// We didn't run keypath migration
				Logger.LogInformation($"{network.CryptoCode}: Scanning the tracked source...");
				using var legacyTx = await legacyRepo.engine.OpenTransaction();
				var scriptsTable = legacyTx.GetTable($"{legacyRepo._Suffix}Scripts");
				await foreach (var row in scriptsTable.Enumerate())
				{
					using (row)
					{
						var keyInfo = legacyRepo.ToObject<KeyPathInformation>(await row.ReadValue());
						hashToTrackedSource.TryAdd(keyInfo.TrackedSource.GetHash().ToString(), keyInfo.TrackedSource);
					}
				}
			}

			if (!progress.MetadataMigrated)
			{
				Logger.LogInformation($"{network.CryptoCode}: Migrating metadata to postgres...");
				using (var tx = await conn.BeginTransactionAsync())
				{
					using var legacyTx = await legacyRepo.engine.OpenTransaction();
					var metadataTable = legacyTx.GetTable($"{legacyRepo._Suffix}Metadata");
					List<InsertMetadata> batch = new List<InsertMetadata>(100);
					await foreach (var row in metadataTable.Enumerate())
					{
						using (row)
						{
							var v = network.Serializer.ToObject<JToken>(legacyRepo.Unzip(await row.ReadValue()));
							var s = Encoding.UTF8.GetString(row.Key.Span).Split('-');
							var trackedSource = hashToTrackedSource[s[0]];
							var key = s[1];
							batch.Add(new InsertMetadata(postgresRepo.GetWalletKey(trackedSource).wid, key, v.ToString(Formatting.None)));
							if (batch.Count == 100)
							{
								await conn.ExecuteAsync("INSERT INTO nbxv1_metadata VALUES (@wallet_id, @key, @value::JSONB)", batch);
								batch.Clear();
							}
						}
					}
					await conn.ExecuteAsync("INSERT INTO nbxv1_metadata VALUES (@wallet_id, @key, @value::JSONB)", batch);
					Logger.LogInformation($"{network.CryptoCode}: metadata migrated.");
					progress.MetadataMigrated = true;
					await SaveProgress(network, conn, progress);
					await tx.CommitAsync();
				};
			}

			if (!progress.HighestPathMigrated)
			{
				Logger.LogInformation($"{network.CryptoCode}: Migrating highest path to postgres...");
				using (var tx = await conn.BeginTransactionAsync())
				{
					var batch = new List<UpdateNextIndex>(100);
					using var legacyTx = await legacyRepo.engine.OpenTransaction();
					var highestPath = legacyTx.GetTable($"{legacyRepo._Suffix}HighestPath");
					await foreach (var row in highestPath.Enumerate())
					{
						using (row)
						{
							var s = Encoding.UTF8.GetString(row.Key.Span).Split('-');
							var feature = Enum.Parse<DerivationFeature>(s[1]);
							var scheme = ((DerivationSchemeTrackedSource)hashToTrackedSource[s[0]]).DerivationStrategy;
							var key = postgresRepo.GetDescriptorKey(scheme, feature);
							var v = await row.ReadValue();
							uint value = System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(v.Span);
							value = value & ~0x8000_0000U;
							batch.Add(new UpdateNextIndex(key.code, key.descriptor, value + 1));
							if (batch.Count == 100)
							{
								await conn.ExecuteAsync("UPDATE descriptors SET next_idx=@next_idx WHERE code=@code AND descriptor=@descriptor", batch);
								batch.Clear();
							}
						}
					}
					await conn.ExecuteAsync("UPDATE descriptors SET next_idx=@next_idx WHERE code=@code AND descriptor=@descriptor", batch);
					Logger.LogInformation($"{network.CryptoCode}: highest path migrated.");
					progress.HighestPathMigrated = true;
					await SaveProgress(network, conn, progress);
					await tx.CommitAsync();
				}
			}

			if (!progress.AvailableKeysMigrated)
			{
				Logger.LogInformation($"{network.CryptoCode}: Migrating available keys to postgres...");
				using var tx = await conn.BeginTransactionAsync();
				var batch = new List<UpdateUsedScript>(100);
				using var legacyTx = await legacyRepo.engine.OpenTransaction();
				var availableTable = legacyTx.GetTable($"{legacyRepo._Suffix}AvailableKeys");
				await foreach (var row in availableTable.Enumerate())
				{
					using (row)
					{
						var s = Encoding.UTF8.GetString(row.Key.Span).Split('-');
						var feature = Enum.Parse<DerivationFeature>(s[1]);
						var scheme = ((DerivationSchemeTrackedSource)hashToTrackedSource[s[0]]).DerivationStrategy;
						var key = postgresRepo.GetDescriptorKey(scheme, feature);
						var idx = int.Parse(s[^1]);
						batch.Add(new UpdateUsedScript(key.code, key.descriptor, idx));
						if (batch.Count == 100)
						{
							await conn.ExecuteAsync("UPDATE descriptors_scripts SET used='f' WHERE code=@code AND descriptor=@descriptor AND idx=@idx", batch);
							batch.Clear();
						}
					}
				}
				await conn.ExecuteAsync("UPDATE descriptors_scripts SET used='f' WHERE code=@code AND descriptor=@descriptor AND idx=@idx", batch);
				progress.AvailableKeysMigrated = true;
				await SaveProgress(network, conn, progress);
				await tx.CommitAsync();
				Logger.LogInformation($"{network.CryptoCode}: Available keys migrated.");
			}

			if (!progress.BlocksMigrated)
			{
				Logger.LogInformation($"{network.CryptoCode}: Migrating blocks to postgres...");
				using var tx = await conn.BeginTransactionAsync();
				HashSet<uint256> blocksToFetch = new HashSet<uint256>();
				using var legacyTx = await legacyRepo.engine.OpenTransaction();
				var savedTxsTable = legacyTx.GetTable($"{legacyRepo._Suffix}Txs");
				await foreach (var row in savedTxsTable.Enumerate())
				{
					using (row)
					{
						if (row.Key.Length == 64)
						{
							blocksToFetch.Add(new uint256(row.Key.Span.Slice(32, 32)));
						}
					}
				}
				var trackedTxs = legacyTx.GetTable($"{legacyRepo._Suffix}Transactions");
				await foreach (var row in trackedTxs.Enumerate())
				{
					using (row)
					{
						var key = TrackedTransactionKey.Parse(row.Key.Span);
						if (key.BlockHash is not null)
							blocksToFetch.Add(key.BlockHash);
					}
				}

				var indexProgress = await legacyRepo.GetIndexProgress(legacyTx);
				await postgresRepo.SetIndexProgress(conn, indexProgress);
				if (indexProgress?.Blocks is not null)
				{
					foreach (var b in indexProgress.Blocks)
						blocksToFetch.Add(b);
				}
				Logger.LogInformation($"{network.CryptoCode}: Blocks to import: " + blocksToFetch.Count);
				foreach (var batch in blocksToFetch.Batch(500))
				{
					var rpc = RpcClients.GetRPCClient(network);
					rpc = rpc.PrepareBatch();
					List<Task<UpdateBlock>> gettingHeaders = new List<Task<UpdateBlock>>();
					foreach (var blk in batch)
					{
						gettingHeaders.Add(GetBlockHeaderAsync(rpc, blk));
					}
					await rpc.SendBatchAsync();

					List<UpdateBlock> update = new List<UpdateBlock>();
					foreach (var gh in gettingHeaders)
					{
						var blockHeader = await gh;
						update.Add(blockHeader);
					}
					await conn.ExecuteAsync("INSERT INTO blks VALUES (@code, @blk_id, @height, @prev_id, 't')", update);
				}

				progress.BlocksMigrated = true;
				await SaveProgress(network, conn, progress);
				await tx.CommitAsync();
				Logger.LogInformation($"{network.CryptoCode}: Blocks migrated.");
			}

			if (!progress.SavedTransactionsMigrated)
			{
				Logger.LogInformation($"{network.CryptoCode}: Migrating saved transactions...");
				using var tx = await conn.BeginTransactionAsync();
				var batchTxs = new List<UpdateTransaction>(1000);
				var batchBlocksTxs = new List<UpdateBlockTransaction>(1000);

				using var legacyTx = await legacyRepo.engine.OpenTransaction();
				var savedTxsTable = legacyTx.GetTable($"{legacyRepo._Suffix}Txs");
				var total = await savedTxsTable.GetRecordCount();
				long migrated = 0;
				await foreach (var row in savedTxsTable.Enumerate())
				{
					migrated++;
					if (migrated % 10_000 == 0)
						Logger.LogInformation($"{network.CryptoCode}: Progress: " + (int)(((double)migrated / (double)total) * 100.0) + "%");
					using (row)
					{
						var savedTx = Repository.ToSavedTransaction(network.NBitcoinNetwork, row.Key, await row.ReadValue());
						if (savedTx.BlockHash is not null)
						{
							batchBlocksTxs.Add(new UpdateBlockTransaction(network.CryptoCode, savedTx.Transaction.GetHash().ToString(), savedTx.BlockHash.ToString()));
						}
						batchTxs.Add(new UpdateTransaction(network.CryptoCode, savedTx.Transaction.GetHash().ToString(), savedTx.Transaction.ToBytes(), savedTx.Timestamp.UtcDateTime));
						if (batchTxs.Count == 1000)
						{
							await conn.ExecuteAsync(
								"INSERT INTO txs AS t (code, tx_id, raw, seen_at) VALUES (@code, @tx_id, @raw, @seen_at) " +
								"ON CONFLICT (code, tx_id) DO UPDATE SET raw=COALESCE(t.raw, EXCLUDED.raw), seen_at=LEAST(t.seen_at, EXCLUDED.seen_at);", batchTxs);

							await conn.ExecuteAsync(
								"INSERT INTO blks_txs " +
								"SELECT @code, @blk_id, @tx_id " +
								"FROM blks " +
								"WHERE code=@code AND blk_id=@blk_id " +
								"ON CONFLICT DO NOTHING;", batchBlocksTxs);
							batchBlocksTxs.Clear();
							batchTxs.Clear();
						}
					}
				}
				await conn.ExecuteAsync(
								"INSERT INTO txs AS t (code, tx_id, raw, seen_at) VALUES (@code, @tx_id, @raw, @seen_at) " +
								"ON CONFLICT (code, tx_id) DO UPDATE SET raw=COALESCE(t.raw, EXCLUDED.raw), seen_at=LEAST(t.seen_at, EXCLUDED.seen_at);", batchTxs);
				await conn.ExecuteAsync(
					"INSERT INTO blks_txs " +
					"SELECT @code, @blk_id, @tx_id " +
					"FROM blks " +
					"WHERE code=@code AND blk_id=@blk_id " +
					"ON CONFLICT DO NOTHING;", batchBlocksTxs);
				batchTxs.Clear();
				batchBlocksTxs.Clear();
				progress.SavedTransactionsMigrated = true;
				await SaveProgress(network, conn, progress);
				await tx.CommitAsync();
				Logger.LogInformation($"{network.CryptoCode}: Saved transactions migrated.");
			}

			if (!progress.TrackedTransactionsOutputsMigrated)
			{
				Logger.LogInformation($"{network.CryptoCode}: Migrating tracked transactions outputs...");
				using var tx = await conn.BeginTransactionAsync();
				using var legacyTx = await legacyRepo.engine.OpenTransaction();
				var savedTxsTable = legacyTx.GetTable($"{legacyRepo._Suffix}Transactions");
				var total = await savedTxsTable.GetRecordCount();
				long migrated = 0;
				List<InsertOuts> batch = new List<InsertOuts>(1000);
				await foreach (var row in savedTxsTable.Enumerate())
				{
					migrated++;
					if (migrated % 10_000 == 0)
						Logger.LogInformation($"{network.CryptoCode}: Progress: " + (int)(((double)migrated / (double)total) * 100.0) + "%");
					using (row)
					{
						TrackedTransaction tt = await ToTrackedTransaction(network, legacyRepo, hashToTrackedSource, row);

						foreach (var o in tt.GetReceivedOutputs())
						{
							long value;
							string assetId;
							if (o.Value is Money m)
							{
								value = m.Satoshi;
								assetId = "";
							}
							else if (o.Value is AssetMoney am)
							{
								value = am.Quantity;
								assetId = am.AssetId.ToString();
							}
							else
								continue;
							batch.Add(new InsertOuts(
								network.CryptoCode,
								tt.TransactionHash.ToString(),
								o.Index,
								o.ScriptPubKey.ToHex(),
								value,
								assetId));

							if (batch.Count > 1000)
							{
								await conn.ExecuteAsync("INSERT INTO outs VALUES (@code, @tx_id, @idx, @script, @value, @asset_id) ON CONFLICT DO NOTHING;", batch);
								batch.Clear();
							}
						}
					}
				}
				await conn.ExecuteAsync("INSERT INTO outs VALUES (@code, @tx_id, @idx, @script, @value, @asset_id) ON CONFLICT DO NOTHING;", batch);
				progress.TrackedTransactionsOutputsMigrated = true;
				await SaveProgress(network, conn, progress);
				await tx.CommitAsync();
				Logger.LogInformation($"{network.CryptoCode}: Tracked transactions outputs migrated.");
			}

			if (!progress.TrackedTransactionsInputsMigrated)
			{
				Logger.LogInformation($"{network.CryptoCode}: Migrating tracked transactions inputs...");
				using var tx = await conn.BeginTransactionAsync();
				using var legacyTx = await legacyRepo.engine.OpenTransaction();
				var savedTxsTable = legacyTx.GetTable($"{legacyRepo._Suffix}Transactions");
				var total = await savedTxsTable.GetRecordCount();
				long migrated = 0;
				List<InsertIns> batch = new List<InsertIns>(1000);
				await foreach (var row in savedTxsTable.Enumerate())
				{
					migrated++;
					if (migrated % 5_000 == 0)
						Logger.LogInformation($"{network.CryptoCode}: Progress: " + (int)(((double)migrated / (double)total) * 100.0) + "%");
					using (row)
					{
						TrackedTransaction tt = await ToTrackedTransaction(network, legacyRepo, hashToTrackedSource, row);
						if (tt.Key.IsPruned)
							continue;
						foreach (var o in tt.SpentOutpoints)
						{
							batch.Add(new InsertIns(
								network.CryptoCode,
								tt.TransactionHash.ToString(),
								tt.IndexOfInput(o),
								o.Hash.ToString(),
								o.N));

							if (batch.Count > 1000)
							{
								await conn.ExecuteAsync(
									"INSERT INTO ins " +
									"SELECT @code, @input_tx_id, @input_idx, @spent_tx_id, @spent_idx FROM outs " +
									"WHERE code=@code AND tx_id=@spent_tx_id AND idx=@spent_idx " +
									"ON CONFLICT DO NOTHING;", batch);
								batch.Clear();
							}
						}
					}
				}
				await conn.ExecuteAsync(
					"INSERT INTO ins " +
					"SELECT @code, @input_tx_id, @input_idx, @spent_tx_id, @spent_idx FROM outs " +
					"WHERE code=@code AND tx_id=@spent_tx_id AND idx=@spent_idx " +
					"ON CONFLICT DO NOTHING;", batch);
				progress.TrackedTransactionsInputsMigrated = true;
				await SaveProgress(network, conn, progress);
				await tx.CommitAsync();
				Logger.LogInformation($"{network.CryptoCode}: Tracked transactions inputs migrated.");
			}
		}

		private static async Task<TrackedTransaction> ToTrackedTransaction(NBXplorerNetwork network, Repository legacyRepo, Dictionary<string, TrackedSource> hashToTrackedSource, DBTrie.IRow row)
		{
			var seg = DBTrie.PublicExtensions.GetUnderlyingArraySegment(await row.ReadValue());
			MemoryStream ms = new MemoryStream(seg.Array, seg.Offset, seg.Count);
			BitcoinStream bs = new BitcoinStream(ms, false);
			bs.ConsensusFactory = network.NBitcoinNetwork.Consensus.ConsensusFactory;
			var trackedSerializable = legacyRepo.CreateBitcoinSerializableTrackedTransaction(TrackedTransactionKey.Parse(row.Key.Span));
			trackedSerializable.ReadWrite(bs);
			var trackedSource = hashToTrackedSource[Encoding.UTF8.GetString(row.Key.Span).Split('-')[0]];
			var tt = legacyRepo.ToTrackedTransaction(trackedSerializable, trackedSource);
			return tt;
		}

		private async Task<UpdateBlock> GetBlockHeaderAsync(NBitcoin.RPC.RPCClient rpc, uint256 blk)
		{
			var header = await rpc.SendCommandAsync("getblockheader", new[] { blk.ToString() });
			if (header.Result is null)
				return null;
			var response = header.Result;
			var confs = response["confirmations"].Value<long>();
			if (confs == -1)
				return null;
			return new UpdateBlock(rpc.Network.NetworkSet.CryptoCode, blk.ToString(), response["previousblockhash"]?.Value<string>(), response["height"].Value<long>());
		}

		private static async Task CreateWalletAndDescriptor(System.Data.Common.DbConnection conn, HashSet<PostgresRepository.WalletKey> walletKeys, List<InsertDescriptor> descriptors)
		{
			await conn.ExecuteAsync("INSERT INTO wallets VALUES (@wid, @metadata::JSONB) ON CONFLICT DO NOTHING;", walletKeys);
			await conn.ExecuteAsync("INSERT INTO descriptors VALUES (@code, @descriptor, @metadata::JSONB) ON CONFLICT DO NOTHING", descriptors);
		}

		private static async Task SaveProgress(NBXplorerNetwork network, System.Data.Common.DbConnection conn, MigrationProgress progress)
		{
			await conn.ExecuteAsync(
									"INSERT INTO nbxv1_settings (code, key, data_json) VALUES (@code, 'MigrationProgress', @data::JSONB) " +
									"ON CONFLICT (code, key) DO " +
									"UPDATE SET data_json=EXCLUDED.data_json;", new { code = network.CryptoCode, data = JsonConvert.SerializeObject(progress) });
		}

		public async Task StopAsync(CancellationToken cancellationToken)
		{
			if (started)
				await LegacyRepositoryProvider.StopAsync(cancellationToken);
		}
	}
}
