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
using System.Runtime.CompilerServices;
using System.IO;
using System.Diagnostics;

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
		[Trait("Benchmark", "Benchmark")]
		public async Task BenchmarkDatabase()
		{
			await using var conn = await GetConnection();
			conn.Execute(GetScript("generate-whale.sql"));
			Logs.WriteLine("Data loaded");
			await Benchmark(conn, "SELECT * FROM wallets_utxos;", 50);
			await Benchmark(conn, "CALL new_block_updated('BTC', 100);", 50);
			await Benchmark(conn, "CALL orphan_blocks('BTC', 1000000);", 200);
			await Benchmark(conn, "SELECT ts.script, ts.addr, ts.source, ts.descriptor, ts.keypath FROM ( VALUES ('BTC', 'blah'), ('BTC', 'blah'), ('BTC', 'blah'), ('BTC', 'blah')) r (code, script), LATERAL (SELECT DISTINCT script, addr, source, descriptor, keypath FROM tracked_scripts ts WHERE ts.code=r.code AND ts.script=r.script) ts;", 50);
			await Benchmark(conn, "SELECT o.tx_id, o.idx, o.value, o.script FROM (VALUES ('BTC', 'hash', 5), ('BTC', 'hash', 5), ('BTC', 'hash', 5))  r (code, tx_id, idx) JOIN outs o USING (code, tx_id, idx);", 50);
		}

		private static string GetScript(string script)
		{

			var directory = new DirectoryInfo(Directory.GetCurrentDirectory());
			while (directory != null && !directory.GetFiles("*.csproj").Any())
			{
				directory = directory.Parent;
			}
			return File.ReadAllText(Path.Combine(directory.FullName, "Scripts", script));
		}

		private async Task Benchmark(DbConnection connection, string script, int target)
		{
			// Warmup
			await connection.ExecuteAsync(script);
			Stopwatch stopwatch = new Stopwatch();
			stopwatch.Start();
			int iterations = 20;
			await connection.ExecuteAsync(string.Join(';', Enumerable.Range(0, iterations).Select(o => script)));
			stopwatch.Stop();
			var ms = ((int)TimeSpan.FromTicks(stopwatch.ElapsedTicks / iterations).TotalMilliseconds);
			Logs.WriteLine(script + " : " + ms + " ms");
			Assert.True(ms < target, "Unacceptable response time for " + script);
		}

		[Fact]
		public async Task CanCalculateUTXO()
		{
			await using var conn = await GetConnection();
			await conn.ExecuteAsync(
				"INSERT INTO wallets VALUES ('Alice');" +
				"INSERT INTO scripts VALUES ('BTC', 'a1', '');" +
				"INSERT INTO wallets_scripts (code, wallet_id, script) VALUES ('BTC', 'Alice', 'a1');" +
				"INSERT INTO txs (code, tx_id, mempool) VALUES ('BTC', 't1', 't');" +
				"INSERT INTO outs VALUES('BTC', 't1', 10, 'a1', 5); ");
			Assert.Single(await conn.QueryAsync("SELECT * FROM wallets_utxos WHERE wallet_id='Alice'"));

			await conn.ExecuteAsync(
				"INSERT INTO blks VALUES ('BTC', 'b1', 0, 'b0');" +
				"INSERT INTO txs_blks (code, tx_id, blk_id) VALUES ('BTC', 't1', 'b1');" +
				"CALL new_block_updated('BTC', 0);");

			Assert.Equal("b1", conn.ExecuteScalar<string>("SELECT blk_id FROM txs WHERE tx_id='t1'"));

			Assert.Single(await conn.QueryAsync("SELECT * FROM wallets_utxos WHERE wallet_id='Alice'"));

			await conn.Orphan("b1");

			Assert.Null(conn.ExecuteScalar<string>("SELECT blk_id FROM txs WHERE tx_id='t1'"));
			await conn.ExecuteAsync("UPDATE blks SET confirmed='t';" +
									"CALL new_block_updated('BTC', 0);");
			Assert.Equal("b1", conn.ExecuteScalar<string>("SELECT blk_id FROM txs WHERE tx_id='t1'"));
			await conn.Orphan("b1");

			var balance = conn.QuerySingle("SELECT * FROM wallets_balances WHERE wallet_id='Alice';");
			Assert.Equal(0, balance.confirmed_balance);

			await conn.ExecuteAsync(
				"INSERT INTO blks VALUES ('BTC', 'b2', 0, 'b0');" +
				"INSERT INTO txs_blks (code, tx_id, blk_id) VALUES ('BTC', 't1', 'b2');" +
				"CALL new_block_updated('BTC', 0);");

			balance = conn.QuerySingle("SELECT * FROM wallets_balances WHERE wallet_id='Alice';");
			Assert.Equal(5, balance.confirmed_balance);
			Assert.Equal(5, balance.available_balance);

			await conn.ExecuteAsync(
				"INSERT INTO txs (code, tx_id, mempool) VALUES ('BTC', 't2', 't');" +
				"INSERT INTO ins VALUES ('BTC', 't2', 0, 't1', 10);");

			balance = conn.QuerySingle("SELECT * FROM wallets_balances WHERE wallet_id='Alice';");
			Assert.Equal(5, balance.confirmed_balance);
			Assert.Equal(0, balance.available_balance);

			await conn.ExecuteAsync("UPDATE txs SET mempool='f' WHERE tx_id='t2'");

			balance = conn.QuerySingle("SELECT * FROM wallets_balances WHERE wallet_id='Alice';");
			Assert.Equal(5, balance.confirmed_balance);
			Assert.Equal(5, balance.available_balance);

			await conn.ExecuteAsync("UPDATE txs SET mempool='t', replaced_by='t1' WHERE tx_id='t2'");
			balance = conn.QuerySingle("SELECT * FROM wallets_balances WHERE wallet_id='Alice';");
			Assert.Equal(5, balance.confirmed_balance);
			Assert.Equal(5, balance.available_balance);

			await conn.ExecuteAsync(
				"INSERT INTO blks VALUES ('BTC', 'b3', 1, 'b2');" +
				"INSERT INTO txs_blks (code, tx_id, blk_id) VALUES ('BTC', 't2', 'b3');" +
				"CALL new_block_updated('BTC', 0);");

			balance = conn.QuerySingleOrDefault("SELECT * FROM wallets_balances WHERE wallet_id='Alice';");
			Assert.Null(balance);
			await conn.Orphan("b3");

			balance = conn.QuerySingle("SELECT * FROM wallets_balances WHERE wallet_id='Alice';");
			Assert.Equal(5, balance.confirmed_balance);
			Assert.Equal(0, balance.available_balance);
		}

		[Fact]
		public async Task CanCalculateUTXO2()
		{
			await using var conn = await GetConnection();
			await conn.ExecuteAsync(
				"INSERT INTO wallets(wallet_id) VALUES ('Alice'), ('Bob');" +
				"INSERT INTO scripts(code, script, addr) VALUES" +
				"('BTC', 'alice1', '')," +
				"('BTC', 'alice2', '')," +
				"('BTC', 'alice3', '')," +
				"('BTC', 'bob1', '')," +
				"('BTC', 'bob2', '')," +
				"('BTC', 'bob3', '');" +
				"INSERT INTO wallets_scripts(code, wallet_id, script) VALUES " +
				"('BTC', 'Alice', 'alice1')," +
				"('BTC', 'Alice', 'alice2')," +
				"('BTC', 'Alice', 'alice3')," +
				"('BTC', 'Bob', 'bob1')," +
				"('BTC', 'Bob', 'bob2')," +
				"('BTC', 'Bob', 'bob3')");


			// 1 coin to alice, 1 to bob
			await conn.ExecuteAsync(
				"INSERT INTO txs (code, tx_id) VALUES ('BTC', 't1'); " +
				"INSERT INTO outs (code, tx_id, idx, script, value) VALUES " +
				"('BTC', 't1', 1, 'alice1', 50), " +
				"('BTC', 't1', 2, 'bob1', 40);" +
				"INSERT INTO blks VALUES ('BTC', 'b1', 1, 'b0');" +
				"INSERT INTO txs_blks (code, blk_id, tx_id) VALUES ('BTC', 'b1', 't1');");

			// alice spend her coin, get change back, 2 outputs to bob
			await conn.ExecuteAsync(
				"INSERT INTO txs (code, tx_id) VALUES ('BTC', 't2'); " +
				"INSERT INTO ins VALUES ('BTC', 't2', 0, 't1', 1);" +
				"INSERT INTO outs (code, tx_id, idx, script, value) VALUES " +
				"('BTC', 't2', 0, 'bob2', 20), " +
				"('BTC', 't2', 1, 'bob3', 39)," +
				"('BTC', 't2', 2, 'alice2', 1);" +
				"INSERT INTO blks VALUES ('BTC', 'b2', 2, 'b1');" +
				"INSERT INTO txs_blks (code, blk_id, tx_id) VALUES ('BTC', 'b2', 't2');" +
				"CALL new_block_updated('BTC', 0);");

			await AssertBalance(conn, "b2", "b1");

			// Replayed on different block.
			await conn.ExecuteAsync(
				"INSERT INTO blks VALUES ('BTC', 'b1-2', 1, 'b0'), ('BTC', 'b2-2', 2, 'b1-2');" +
				"INSERT INTO txs_blks (code, blk_id, tx_id) VALUES ('BTC', 'b1-2', 't1'), ('BTC', 'b2-2', 't2');" +
				"CALL new_block_updated('BTC', 0);");
			await AssertBalance(conn, "b2-2", "b1-2");

			// And again!
			await conn.ExecuteAsync(
				"INSERT INTO blks VALUES ('BTC', 'b1-3', 1, 'b0'), ('BTC', 'b2-3', 2, 'b1-3');" +
				"INSERT INTO txs_blks (code, blk_id, tx_id) VALUES ('BTC', 'b1-3', 't1'), ('BTC', 'b2-3', 't2');" +
				"CALL new_block_updated('BTC', 0);");
			await AssertBalance(conn, "b2-3", "b1-3");

			// Let's test: If the outputs are double spent, then it should disappear from the wallet balance.
			await conn.ExecuteAsync(
				"INSERT INTO txs (code, tx_id) VALUES ('BTC', 'ds'); " +
				"INSERT INTO ins VALUES " +
				"('BTC', 'ds', 0, 't1', 1);" + // This one double spend t2
				"INSERT INTO blks VALUES ('BTC', 'bs', 1, 'b0');" +
				"INSERT INTO txs_blks (code, blk_id, tx_id) VALUES ('BTC', 'bs', 'ds');" +
				"CALL new_block_updated('BTC', 0);");

			// Alice should have her t1 output spent by the confirmed bs, so she has nothing left
			var balance = await conn.QueryFirstOrDefaultAsync("SELECT * FROM wallets_balances WHERE wallet_id='Alice';");
			Assert.Null(balance);

			// Bob should have t1 unconfirmed
			balance = await conn.QueryFirstOrDefaultAsync("SELECT * FROM wallets_balances WHERE wallet_id='Bob';");
			Assert.Equal(40, balance.unconfirmed_balance);
			Assert.Equal(0, balance.confirmed_balance);
			Assert.Equal(40, balance.available_balance);

			Assert.Single(await conn.QueryAsync("SELECT * FROM txs WHERE tx_id='t2' AND mempool IS FALSE AND replaced_by='ds';"));
		}

		private static async Task AssertBalance(DbConnection conn, string b2, string b1)
		{
			// This will check that there is 4 utxo in total
			// 3 for bobs, 1 for alice, then check what happen after
			// orphaning b2 and b1
			Assert.Single(await conn.QueryAsync("SELECT * FROM wallets_utxos WHERE wallet_id='Alice' AND spending_tx_id IS NULL;"));
			var utxos = (await conn.QueryAsync("SELECT * FROM wallets_utxos WHERE wallet_id='Bob' AND spending_tx_id IS NULL;")).ToList();
			Assert.Equal(3, utxos.Count);

			var balance = await conn.QueryFirstOrDefaultAsync("SELECT * FROM wallets_balances WHERE wallet_id='Alice';");
			Assert.Equal(1, balance.unconfirmed_balance);
			Assert.Equal(1, balance.confirmed_balance);
			Assert.Equal(1, balance.available_balance);

			balance = await conn.QueryFirstOrDefaultAsync("SELECT * FROM wallets_balances WHERE wallet_id='Bob';");
			Assert.Equal(40 + 20 + 39, balance.unconfirmed_balance);
			Assert.Equal(40 + 20 + 39, balance.confirmed_balance);
			Assert.Equal(40 + 20 + 39, balance.available_balance);

			await conn.Orphan(b2);

			balance = await conn.QueryFirstOrDefaultAsync("SELECT * FROM wallets_balances WHERE wallet_id='Alice';");
			Assert.Equal(1, balance.unconfirmed_balance);
			Assert.Equal(50, balance.confirmed_balance);
			Assert.Equal(1, balance.available_balance);

			balance = await conn.QueryFirstOrDefaultAsync("SELECT * FROM wallets_balances WHERE wallet_id='Bob';");
			Assert.Equal(40 + 20 + 39, balance.unconfirmed_balance);
			Assert.Equal(40, balance.confirmed_balance);
			Assert.Equal(40 + 20 + 39, balance.available_balance);

			Assert.Single(await conn.QueryAsync("SELECT * FROM wallets_utxos WHERE wallet_id='Alice' AND spent_mempool IS FALSE AND immature IS FALSE;"));

			await conn.Orphan(b1);

			balance = await conn.QueryFirstOrDefaultAsync("SELECT * FROM wallets_balances WHERE wallet_id='Alice';");
			Assert.Equal(1, balance.unconfirmed_balance);
			Assert.Equal(0, balance.confirmed_balance);
			Assert.Equal(1, balance.available_balance);

			balance = await conn.QueryFirstOrDefaultAsync("SELECT * FROM wallets_balances WHERE wallet_id='Bob';");
			Assert.Equal(40 + 20 + 39, balance.unconfirmed_balance);
			Assert.Equal(0, balance.confirmed_balance);
			Assert.Equal(40 + 20 + 39, balance.available_balance);

			Assert.Empty(await conn.QueryAsync("SELECT * FROM wallets_utxos WHERE wallet_id='Alice' AND mempool IS FALSE;"));
		}

		[Fact]
		public async Task CanRunMigrateTwice()
		{
			var db = $"CanRunMigrateTwice_{RandomUtils.GetUInt32()}";
			await using (var conn = await GetConnection(db))
			{
			}
			await using (var conn = await GetConnection(db))
			{
			}
		}

		private async Task<DbConnection> GetConnection(string dbName = null, [CallerMemberName] string applicationName = null)
		{
			var connectionString = ServerTester.GetTestPostgres(dbName, applicationName);
			var conf = new ConfigurationBuilder().AddInMemoryCollection(new[] { new KeyValuePair<string, string>("POSTGRES", connectionString) }).Build();
			var container = new ServiceCollection();
			container.AddSingleton<IConfiguration>(conf);
			container.AddLogging(builder =>
			{
				builder.AddFilter("System.Net.Http.HttpClient", LogLevel.Error);
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
			Assert.Empty(await GetUTXOs(db, walletId));
		}

		public record UTXORow(System.String code, System.String tx_id, System.Int32 idx, System.String script, System.Int64 value, System.String blk_id, System.Int64 height);
		public static async Task<UTXORow[]> GetUTXOs(this DbConnection db, string walletId)
		{
			return (await db.QueryAsync<UTXORow>("SELECT code, tx_id, idx, script, value, blk_id, height FROM wallets_utxos WHERE code='BTC' AND wallet_id=@wid", new { wid = walletId })).ToArray();
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
			var height = await db.ExecuteScalarAsync<long>("SELECT height FROM blks WHERE blk_id=@block", new { block });
			await db.ExecuteAsync("CALL orphan_blocks('BTC', @height);", new { height });
		}
		public static async Task AddWalletOutput(this DbConnection db, string walletId, string tx, int index, string scriptpubkey, int val)
		{
			await CreateTransaction(db, tx);
			await AddOutput(db, tx, index, scriptpubkey, val);
			await CreateWallet(db, walletId);
			await db.ExecuteAsync("INSERT INTO wallets_scripts VALUES ('BTC', @script, @wallet_id) ON CONFLICT DO NOTHING", new { wallet_id = walletId, script = scriptpubkey });
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
			await db.ExecuteAsync("INSERT INTO ins VALUES ('BTC', @tx, 0, @spenttx, @spentidx)", new { tx, spenttx = spentTx, spentidx = spentIndex });
		}
	}
}
