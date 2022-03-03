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
			await conn.AddWalletOutput("Alice", "t1", 10, "a1", 5);
			await conn.AssertEmptyWallet("Alice");

			await conn.ConfirmTx("b1", "t1");
			Assert.Equal("b1", conn.ExecuteScalar<string>("SELECT blk_id FROM txs WHERE tx_id='t1'"));

			var utxo = Assert.Single(await conn.GetUTXOs("Alice"));

			await conn.Orphan("b1");
			Assert.Null(conn.ExecuteScalar<string>("SELECT blk_id FROM txs WHERE tx_id='t1'"));
			await conn.ExecuteAsync("UPDATE blks SET confirmed='t'");
			Assert.Equal("b1", conn.ExecuteScalar<string>("SELECT blk_id FROM txs WHERE tx_id='t1'"));
			await conn.Orphan("b1");
			await conn.AssertEmptyWallet("Alice");
			await conn.ConfirmTx("b2", "t1");

			utxo = Assert.Single(await conn.GetUTXOs("Alice"));
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
			await conn.AddWalletOutput("Alice", "t1", 1, "alice1", 50);
			await conn.AddWalletOutput("Bob", "t1", 2, "bob1", 40);
			await conn.ConfirmTx("b1", "t1");

			// alice spend her coin, get change back, 2 outputs to bob
			await conn.SpendOutput("t2", "t1", 1);
			await conn.AddWalletOutput("Bob", "t2", 1, "bob2", 20);
			await conn.AddWalletOutput("Bob", "t2", 2, "bob3", 39);
			await conn.AddWalletOutput("Alice", "t2", 0, "alice2", 1);
			await conn.ConfirmTx("b2", "t2");

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

			await conn.AddWalletOutput("Alice", "t3", 0, "alice3", 394);
			await conn.ConfirmTx("b3-4", "t3");

			var row = await conn.QueryAsync("SELECT * FROM get_wallet_conf_utxos('BTC', 'Alice')");
		}

		private static async Task AssertBalance(DbConnection conn, string b2, string b1)
		{
			// This will check that there is 4 utxo in total
			// 3 for bobs, 1 for alice, then check what happen after
			// orphaning b2 and b1
			var utxos = await conn.GetUTXOs("Alice");
			Assert.Single(utxos);
			utxos = await conn.GetUTXOs("Bob");
			Assert.Equal(3, utxos.Length);
			await conn.Orphan(b2);
			utxos = await conn.GetUTXOs("Alice");
			Assert.Single(utxos);
			//Assert.Equal(50, utxos[0].value);
			utxos = await conn.GetUTXOs("Bob");
			Assert.Single(utxos);
			//Assert.Equal(40, utxos[0].value);
			await conn.Orphan(b1);
			Assert.Empty(await conn.GetUTXOs("Bob"));
			Assert.Empty(await conn.GetUTXOs("Alice"));
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
			var connectionString = ServerTester.GetTestPostgres(dbName);
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
			await db.ExecuteAsync("INSERT INTO wallets VALUES (@walletid) ON CONFLICT DO NOTHING", new { walletid = walletId });
		}
		public static async Task AssertEmptyWallet(this DbConnection db, string walletId)
		{
			Assert.Equal(0, await db.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM get_wallet_conf_utxos('BTC', @wid)", new { wid = walletId }));
		}

		public record UTXORow(System.String code, System.String tx_id, System.Int32 idx, System.String script, System.Int64 value, System.String blk_id, System.Int64 height);
		public static async Task<UTXORow[]> GetUTXOs(this DbConnection db, string walletId)
		{
			return (await db.QueryAsync<UTXORow>("SELECT code, tx_id, idx, script, value, blk_id, height FROM get_wallet_conf_utxos('BTC', @wid)", new { wid = walletId })).ToArray();
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
		public static async Task AddWalletOutput(this DbConnection db, string walletId, string tx, int index, string scriptpubkey, int val)
		{
			await CreateTransaction(db, tx);
			await AddOutput(db, tx, index, scriptpubkey, val);
			await CreateWallet(db, walletId);
			await db.ExecuteAsync("INSERT INTO scripts_wallets VALUES ('BTC', @script, @wallet_id) ON CONFLICT DO NOTHING", new { wallet_id = walletId, script = scriptpubkey });
		}

		static async Task CreateTransaction(this DbConnection db, string tx)
		{
			await db.ExecuteAsync("INSERT INTO txs VALUES ('BTC', @tx, '') ON CONFLICT DO NOTHING", new { tx });
		}

		public static async Task AddOutput(this DbConnection db, string tx, int index, string scriptpubkey, int val)
		{
			await CreateTransaction(db, tx);
			await db.ExecuteAsync("INSERT INTO scripts VALUES ('BTC', @scriptpubkey, @scriptpubkey) ON CONFLICT DO NOTHING", new { scriptpubkey = scriptpubkey });
			await db.ExecuteAsync("INSERT INTO outs VALUES ('BTC', @tx, @idx, @scriptpubkey, @v)", new { tx, idx = index, scriptpubkey = scriptpubkey, v = val });
		}
		public static async Task SpendOutput(this DbConnection db, string tx, string spentTx, int spentIndex)
		{
			await CreateTransaction(db, tx);
			await db.ExecuteAsync("INSERT INTO ins VALUES ('BTC', @tx, @spenttx, @spentidx)", new { tx, spenttx = spentTx, spentidx = spentIndex });
		}
	}
}
