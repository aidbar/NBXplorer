using System;
using Dapper;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Npgsql;
using NBitcoin;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;
using Xunit.Abstractions;
using Microsoft.Extensions.Configuration;
using NBXplorer.Configuration;

namespace NBXplorer.Tests
{
	public class DatabaseTests
	{
		public DatabaseTests(ITestOutputHelper logs)
		{
			Logs = logs;
		}

		public ITestOutputHelper Logs { get; }

		[Fact]
		public async Task CanCalculateUTXO()
		{
			await using var conn = await GetConnection();
			await conn.CreateWallet("Alice");
			await conn.CreateOutput("Alice", "t1", 10, "a1", 5);
			await conn.AssertEmptyWallet("Alice");

			await conn.ConfirmTx("b1", "t1");

			var utxo = Assert.Single(await conn.GetUTXOs("Alice"));

			await conn.Orphan("b1");
			await conn.AssertEmptyWallet("Alice");
			await conn.ConfirmTx("b2", "t1");

			utxo = Assert.Single(await conn.GetUTXOs("Alice"));
			await conn.CreateTransaction("t2");
			await conn.SpendOutput("t2", "t1", 10);
			utxo = Assert.Single(await conn.GetUTXOs("Alice"));
			await conn.ConfirmTx("b3", "t2");

			await conn.AssertEmptyWallet("Alice");
			await conn.Orphan("b3");

			utxo = Assert.Single(await conn.GetUTXOs("Alice"));
		}

		[Fact]
		public async Task CanCalculateUTXO2()
		{
			await using var conn = await GetConnection();
			await conn.CreateWallet("Alice");
			await conn.CreateWallet("Bob");

			// 1 coin to alice, 1 to bob
			await conn.CreateTransaction("t1");
			await conn.AddOutput("t1", 1, "alice1", 50);
			await conn.AddOutput("t1", 2, "bob1", 40);
			await conn.ConfirmTx("b1", "t1");

			// alice spend her coin, get change back, 2 outputs to bob
			await conn.CreateTransaction("t2");
			await conn.SpendOutput("t2", "t1", 1);
			await conn.AddOutput("t2", 1, "bob2", 20);
			await conn.AddOutput("t2", 2, "bob3", 39);
			await conn.AddOutput("t2", 0, "alice2", 1);
			await conn.ConfirmTx("b2", "t2");

			await conn.AddOutputToWallet("Alice", "t1", 1);
			await conn.AddOutputToWallet("Alice", "t2", 0);
			await conn.AddOutputToWallet("Bob", "t1", 2);
			await conn.AddOutputToWallet("Bob", "t2", 1);
			await conn.AddOutputToWallet("Bob", "t2", 2);

			await AssertBalance(conn, "b2", "b1");

			// Replayed on different block.
			await conn.ConfirmTx("b1-2", "t1");
			await conn.ConfirmTx("b2-2", "t2");
			await AssertBalance(conn, "b2-2", "b1-2");

			// And again!
			await conn.ConfirmTx("b1-3", "t1");
			await conn.ConfirmTx("b2-3", "t2");
			await AssertBalance(conn, "b2-3", "b1-3");

			await conn.ConfirmTx("b1-4", "t1");
			await conn.ConfirmTx("b2-4", "t2");
			await conn.CreateTransaction("t3");
			await conn.AddOutput("t3", 0, "alice3", 394);
			await conn.ConfirmTx("b3-4", "t3");

			var row = await conn.QueryAsync("SELECT * FROM get_wallet_conf_utxos('BTC', 'Alice')");
		}

		private static async Task AssertBalance(DbConnection conn, string b2, string b1)
		{
			// This will check that there is 4 utxo in total
			// 3 for bobs, 1 for alice, then check what happen after
			// orphaning b2 and b1
			var utxos = await conn.GetUTXOs();
			Assert.Equal(4, utxos.Length);
			utxos = await conn.GetUTXOs("Alice");
			Assert.Single(utxos);
			utxos = await conn.GetUTXOs("Bob");
			Assert.Equal(3, utxos.Length);
			await conn.Orphan(b2);
			utxos = await conn.GetUTXOs();
			Assert.Equal(2, utxos.Length);
			utxos = await conn.GetUTXOs("Alice");
			Assert.Single(utxos);
			//Assert.Equal(50, utxos[0].value);
			utxos = await conn.GetUTXOs("Bob");
			Assert.Single(utxos);
			//Assert.Equal(40, utxos[0].value);
			await conn.Orphan(b1);
			Assert.Empty(await conn.GetUTXOs());
		}

		[Fact]
		public async Task CanRunMigrateTwice()
		{
			var db = $"dbtest{RandomUtils.GetUInt32()}";
			await using (var conn = await GetConnection(db))
			{
			}
			await using (var conn = await GetConnection(db))
			{
			}
		}

		private async Task<DbConnection> GetConnection(string dbName = null)
		{
			dbName ??= $"dbtest{RandomUtils.GetUInt32()}";
			var connectionString = $"User ID=postgres;Host=localhost;Include Error Detail=true;Port=39383;Database={dbName}";
			var conf = new ConfigurationBuilder().AddInMemoryCollection(new[] { new KeyValuePair<string, string>("POSTGRES", connectionString) }).Build();
			var container = new ServiceCollection();
			container.AddSingleton<IConfiguration>(conf);
			container.AddLogging(builder =>
			{
				builder.AddProvider(new XUnitLoggerProvider(Logs));
			});
			new Startup(conf).ConfigureServices(container);
			var provider = container.BuildServiceProvider();
			foreach (var service in provider.GetServices<IHostedService>())
				await service.StartAsync(default);
			var facto = provider.GetRequiredService<DbConnectionFactory>();
			return await facto.CreateConnection();
		}
	}
	static class Helper
	{
		public static async Task CreateWallet(this DbConnection db, string walletId)
		{
			await db.ExecuteAsync("INSERT INTO wallets VALUES (@walletid)", new { walletid = walletId });
		}
		public static async Task AssertEmptyWallet(this DbConnection db, string walletId)
		{
			Assert.Equal(0, await db.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM conf_utxos WHERE wallet_id = @wid", new { wid = walletId }));
		}

		public record UTXORow(string code, System.String wallet_id, System.String tx_id, System.Int32 idx, string blk_id);
		public static async Task<UTXORow[]> GetUTXOs(this DbConnection db, string walletId = null)
		{
			if (walletId is String)
				return (await db.QueryAsync<UTXORow>("SELECT * FROM conf_utxos WHERE code = 'BTC' AND wallet_id = @wid", new { wid = walletId })).ToArray();
			else
				return (await db.QueryAsync<UTXORow>("SELECT * FROM conf_utxos")).ToArray();
		}
		public static async Task ConfirmTx(this DbConnection db, string block, string tx)
		{
			await AddBlock(db, block);
			await AddTxToBlock(db, block, tx);
		}

		public static async Task AddTxToBlock(this DbConnection db, string block, string tx)
		{
			await db.ExecuteAsync("INSERT INTO txs_blks VALUES ('BTC', @tx, @blk)", new { blk = block, tx = tx });
		}

		public static async Task AddBlock(this DbConnection db, string block)
		{
			await db.ExecuteAsync("INSERT INTO blks VALUES ('BTC', @blk, 0, 'b0')", new { blk = block });
		}

		public static async Task Orphan(this DbConnection db, string block)
		{
			await db.ExecuteAsync("UPDATE blks SET confirmed = 'f' WHERE blk_id = @b;", new { b = block });
		}
		public static async Task CreateOutput(this DbConnection db, string walletId, string tx, int index, string scriptpubkey, int val)
		{
			await CreateTransaction(db, tx);
			await AddOutput(db, tx, index, scriptpubkey, val);
			await AddOutputToWallet(db, walletId, tx, index);
		}

		public static async Task CreateTransaction(this DbConnection db, string tx)
		{
			await db.ExecuteAsync("INSERT INTO txs VALUES ('BTC', @tx, '')", new { tx });
		}

		public static async Task AddOutput(this DbConnection db, string tx, int index, string scriptpubkey, int val)
		{
			await db.ExecuteAsync("INSERT INTO scripts VALUES ('BTC', @scriptpubkey, 'lol')", new { scriptpubkey = scriptpubkey });
			await db.ExecuteAsync("INSERT INTO outs VALUES ('BTC', @tx, @idx, @scriptpubkey, @v)", new { tx, idx = index, scriptpubkey = scriptpubkey, v = val });
		}

		public static async Task AddOutputToWallet(this DbConnection db, string walletId, string tx, int index)
		{
			await db.ExecuteAsync("INSERT INTO wallets_outs VALUES (@wid, 'BTC', @tx, @idx)", new { wid = walletId, tx, idx = index });
		}

		public static async Task SpendOutput(this DbConnection db, string tx, string spentTx, int spentIndex)
		{
			await db.ExecuteAsync("INSERT INTO ins VALUES ('BTC', @tx, @spenttx, @spentidx)", new { tx, spenttx = spentTx, spentidx = spentIndex });
		}
	}
}
