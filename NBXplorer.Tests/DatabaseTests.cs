﻿using System;
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
using System.Globalization;

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
			await Benchmark(conn,
				"SELECT ts.script, ts.addr, ts.descriptor, ts.keypath FROM ( VALUES ('BTC', 'blah'), ('BTC', 'blah'), ('BTC', 'blah'), ('BTC', 'blah')) r (code, script), " +
				" LATERAL(" +
				"	SELECT ws.script, s.addr, descriptor, ds.keypath " +
				"	FROM wallets_scripts ws " +
				"	LEFT JOIN descriptors_scripts ds USING (code, descriptor, idx) " +
				"	JOIN scripts s ON s.code=ws.code AND s.script=ws.script " +
				"   WHERE ws.code=r.code AND ws.script=r.script) ts;", 50);
			await Benchmark(conn, "SELECT o.tx_id, o.idx, o.value, o.script FROM (VALUES ('BTC', 'hash', 5), ('BTC', 'hash', 5), ('BTC', 'hash', 5))  r (code, tx_id, idx) JOIN outs o USING (code, tx_id, idx);", 50);
			await Benchmark(conn, "SELECT height, tx_id, wu.idx, value, script, keypath, mempool, spent_mempool, seen_at  FROM wallets_utxos wu JOIN descriptors_scripts USING (code, script) WHERE code='BTC' AND wallet_id='WHALE' AND immature IS FALSE", 50);
			await Benchmark(conn, "SELECT * FROM get_wallets_histogram('WHALE', 'BTC', '', '2022-01-01'::timestamptz, '2022-02-01'::timestamptz, interval '1 day');", 50);
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
		public async Task CanCalculateHistogram()
		{
			await using var conn = await GetConnection();
			int txcount = 0;
			int blkcount = 0;
			await conn.ExecuteAsync(
				"INSERT INTO wallets VALUES ('Alice');" +
				"INSERT INTO scripts VALUES ('BTC', 'a1', 'a1');" +
				"INSERT INTO wallets_scripts (code, wallet_id, script) VALUES ('BTC', 'Alice', 'a1');"
				);

			async Task Receive(long value, DateTimeOffset date)
			{
				await conn.ExecuteAsync(
				"INSERT INTO txs (code, tx_id, mempool, seen_at) VALUES ('BTC', @tx, 't', @seen_at);" +
				"INSERT INTO outs VALUES ('BTC', @tx, 0, 'a1', @val);" +
				"INSERT INTO blks VALUES ('BTC', @blk, @blkcount, 'f');" +
				"INSERT INTO blks_txs (code, tx_id, blk_id) VALUES ('BTC', @tx, @blk);" +
				"UPDATE blks SET confirmed='t' WHERE code='BTC' AND blk_id=@blk;" +
				"CALL new_block_updated('BTC', 101);",
				new
				{
					tx = "tx" + (txcount++),
					blk = "b" + (blkcount++),
					blkcount = blkcount - 1,
					val = value,
					seen_at = date
				});
			}

			async Task Spend(long value, DateTimeOffset date)
			{
				var utxos = await conn.QueryAsync("SELECT * FROM wallets_utxos WHERE wallet_id='Alice';");
				long total = 0;
				List<(string tx_id, long idx)> spent = new List<(string tx_id, long idx)>();
				foreach (var utxo in utxos)
				{
					while (total < value)
					{
						spent.Add((utxo.tx_id, utxo.idx));
						total += utxo.value;
					}
				}
				Assert.True(total > value, "Not enough money");

				var parameters = new
				{
					tx = "tx" + (txcount++),
					blk = "b" + (blkcount++),
					blkcount = blkcount - 1,
					change = total - value,
					seen_at = date
				};
				await conn.ExecuteAsync("INSERT INTO txs (code, tx_id, mempool, seen_at) VALUES ('BTC', @tx, 't', @seen_at);", parameters);
				if (parameters.change != 0)
				{
					await conn.ExecuteAsync("INSERT INTO outs VALUES ('BTC', @tx, 0, 'a1', @change);", parameters);
				}
				int i = 0;
				foreach (var s in spent)
				{
					await conn.ExecuteAsync("INSERT INTO ins VALUES ('BTC', @tx, @idx, @spent_tx, @spent_idx);", new
					{
						tx = parameters.tx,
						idx = i,
						spent_tx = s.tx_id,
						spent_idx = s.idx
					});
					i++;
				}
				await conn.ExecuteAsync(
				"INSERT INTO blks VALUES ('BTC', @blk, @blkcount, 'f');" +
				"INSERT INTO blks_txs (code, tx_id, blk_id) VALUES ('BTC', @tx, @blk);" +
				"UPDATE blks SET confirmed='t' WHERE code='BTC' AND blk_id=@blk;" +
				"CALL new_block_updated('BTC', 101);",
				parameters);
			}

			var date = new DateTime(2022, 1, 1, 0, 0, 0, DateTimeKind.Utc);
			var from = date;
			await Receive(50, date);
			date += TimeSpan.FromMinutes(2);  // 2
			await Spend(20, date);
			date += TimeSpan.FromMinutes(10); // 12
			await Spend(10, date);
			date += TimeSpan.FromMinutes(1); // 13
			await Receive(10, date);
			date += TimeSpan.FromMinutes(8); // 21
			await Receive(30, date);
			date += TimeSpan.FromMinutes(20); // 41
			await Spend(50, date);
			date += TimeSpan.FromMinutes(10); // 51
			await Receive(2, date);
			date += TimeSpan.FromMinutes(2); // 53
			await Spend(1, date);
			await conn.ExecuteAsync("REFRESH MATERIALIZED VIEW wallets_history;");

			var r1 = await conn.QueryAsync("SELECT * FROM get_wallets_histogram('Alice', 'BTC', '', @from, @to, interval '5 minutes')", new
			{
				from = from,
				to = from + TimeSpan.FromHours(1.0)
			});

			var expected = new (long, long)[]
			{
				(30,30),
				(0,30),
				(0,30),
				(0,30),
				(30,60),
				(0,60),
				(0,60),
				(0,60),
				(-50,10),
				(0,10),
				(1,11),
				(0,11)
			};
			foreach (var pair in Enumerable.Zip(r1.Select(o => ((long)o.balance_change, (long)o.balance)),
						expected))
			{
				Assert.Equal(pair.First, pair.Second);
			}

			r1 = await conn.QueryAsync("SELECT * FROM get_wallets_histogram('Alice', 'BTC', '', @from, @to, interval '5 minutes')", new
			{
				from = from + TimeSpan.FromMinutes(30.0),
				to = from + TimeSpan.FromHours(1.0)
			});

			expected = expected.Skip(6).ToArray();
			foreach (var pair in Enumerable.Zip(r1.Select(o => ((long)o.balance_change, (long)o.balance)),
						expected))
			{
				Assert.Equal(pair.First, pair.Second);
			}
		}

		[Fact]
		public async Task CanDetectDoubleSpending()
		{
			await using var conn = await GetConnection();
			// t0 has an output, then t1 spend it, followed by t2.
			// t1 should be marked replaced_by
			// then t3 spend the input
			// t2 should be marked replaced_by
			await conn.ExecuteAsync(
				"INSERT INTO txs (code, tx_id, mempool) VALUES ('BTC', 't0', 't'), ('BTC', 't1', 't'),  ('BTC', 't2', 't'), ('BTC', 't3', 't'), ('BTC', 't4', 't'), ('BTC', 't5', 't');" +
				"INSERT INTO scripts VALUES ('BTC', 'a1', '');" +
				"INSERT INTO outs VALUES ('BTC', 't0', 10, 'a1', 5);" +
				"INSERT INTO ins VALUES ('BTC', 't1', 0, 't0', 10);" +
				"INSERT INTO ins VALUES ('BTC', 't2', 0, 't0', 10);"
				);

			var t1 = await conn.QueryFirstAsync("SELECT * FROM txs WHERE tx_id='t1'");
			Assert.True(t1.mempool);
			Assert.Equal("t2", t1.replaced_by);

			var t2 = await conn.QueryFirstAsync("SELECT * FROM txs WHERE tx_id='t2'");
			Assert.True(t2.mempool);
			Assert.Null(t2.replaced_by);

			await conn.ExecuteAsync("INSERT INTO ins VALUES ('BTC', 't3', 0, 't0', 10);");
			t2 = await conn.QueryFirstAsync("SELECT * FROM txs WHERE tx_id='t2'");
			Assert.True(t2.mempool);
			Assert.Equal("t3", t2.replaced_by);

			// Does it propagate to other children? t3 get spent by t4 then t3 get double spent by t5.
			// We expect t3 and t4 to be double spent
			await conn.ExecuteAsync("INSERT INTO outs VALUES('BTC', 't3', 10, 'a1', 5);" +
				"INSERT INTO ins VALUES ('BTC', 't4', 0, 't3', 10);" +
				"INSERT INTO ins VALUES ('BTC', 't5', 0, 't0', 10);");

			var t3 = await conn.QueryFirstAsync("SELECT * FROM txs WHERE tx_id='t3'");
			Assert.True(t3.mempool);
			Assert.Equal("t5", t3.replaced_by);

			var t4 = await conn.QueryFirstAsync("SELECT * FROM txs WHERE tx_id='t4'");
			Assert.True(t4.mempool);
			Assert.Equal("t5", t4.replaced_by);

			var t5 = await conn.QueryFirstAsync("SELECT * FROM txs WHERE tx_id='t5'");
			Assert.True(t5.mempool);
			Assert.Null(t5.replaced_by);
		}

		[Fact]
		public async Task CanCalculateUTXO()
		{
			await using var conn = await GetConnection();
			await conn.ExecuteAsync(
				"INSERT INTO wallets VALUES ('Alice');" +
				"INSERT INTO scripts VALUES ('BTC', 'a1', '');" +
				"INSERT INTO wallets_scripts VALUES ('BTC', 'a1', 'Alice');" +
				"INSERT INTO txs (code, tx_id, mempool) VALUES ('BTC', 't1', 't');" +
				"INSERT INTO outs VALUES('BTC', 't1', 10, 'a1', 5); ");
			Assert.Single(await conn.QueryAsync("SELECT * FROM wallets_utxos WHERE wallet_id='Alice'"));

			await conn.ExecuteAsync(
				"INSERT INTO blks VALUES ('BTC', 'b1', 0, 'b0');" +
				"INSERT INTO blks_txs (code, tx_id, blk_id) VALUES ('BTC', 't1', 'b1');" +
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
				"INSERT INTO blks_txs (code, tx_id, blk_id) VALUES ('BTC', 't1', 'b2');" +
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
				"INSERT INTO blks_txs (code, tx_id, blk_id) VALUES ('BTC', 't2', 'b3');" +
				"CALL new_block_updated('BTC', 0);");

			balance = conn.QuerySingleOrDefault("SELECT * FROM wallets_balances WHERE wallet_id='Alice';");
			Assert.Null(balance);
			await conn.Orphan("b3");

			balance = conn.QuerySingle("SELECT * FROM wallets_balances WHERE wallet_id='Alice';");
			Assert.Equal(5, balance.confirmed_balance);
			Assert.Equal(0, balance.available_balance);
		}

		[Fact]
		public async Task CanMempoolPropagate()
		{
			await using var conn = await GetConnection();
			// t1 get spent by t2 then t2 by t3. But then, t4 double spend t2 and get validated.
			// So t2 and t3 should get out of mempool.
			await conn.ExecuteAsync(
				"INSERT INTO txs (code, tx_id, mempool) VALUES ('BTC', 't1', 't'), ('BTC', 't2', 't'), ('BTC', 't3', 't'), ('BTC', 't4', 't');" +
				"INSERT INTO scripts VALUES ('BTC', 'script', '');" +
				"INSERT INTO outs (code, tx_id, idx, script, value) VALUES ('BTC', 't1', 0, 'script', 5), ('BTC', 't2', 0, 'script', 5);" +
				"INSERT INTO ins VALUES ('BTC', 't2', 0, 't1', 0);" +
				"INSERT INTO ins VALUES ('BTC', 't3', 0, 't2', 0);" +
				"INSERT INTO ins VALUES ('BTC', 't4', 0, 't1', 0);" +
				"INSERT INTO blks (code, blk_id, height, prev_id) VALUES ('BTC', 'b1', 1, 'b0');" +
				"INSERT INTO blks_txs (code, blk_id, tx_id) VALUES ('BTC', 'b1', 't4'), ('BTC', 'b1', 't1');" +
				"CALL new_block_updated('BTC', 0);");

			var t3 = await conn.QueryFirstAsync("SELECT * FROM txs WHERE tx_id='t3'");
			Assert.False(t3.mempool);
			Assert.Null(t3.blk_id);
			Assert.Equal("t4", t3.replaced_by);

			var t2 = await conn.QueryFirstAsync("SELECT * FROM txs WHERE tx_id='t2'");
			Assert.False(t2.mempool);
			Assert.Null(t2.blk_id);
			Assert.Equal("t4", t2.replaced_by);

			var t1 = await conn.QueryFirstAsync("SELECT * FROM txs WHERE tx_id='t1'");
			Assert.False(t1.mempool);
			Assert.Equal("b1", t1.blk_id);

			var t4 = await conn.QueryFirstAsync("SELECT * FROM txs WHERE tx_id='t4'");
			Assert.False(t4.mempool);
			Assert.Equal("b1", t4.blk_id);
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
				"INSERT INTO wallets_scripts (code, wallet_id, script) VALUES " +
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
				"INSERT INTO blks_txs (code, blk_id, tx_id) VALUES ('BTC', 'b1', 't1');");

			// alice spend her coin, get change back, 2 outputs to bob
			await conn.ExecuteAsync(
				"INSERT INTO txs (code, tx_id) VALUES ('BTC', 't2'); " +
				"INSERT INTO ins VALUES ('BTC', 't2', 0, 't1', 1);" +
				"INSERT INTO outs (code, tx_id, idx, script, value) VALUES " +
				"('BTC', 't2', 0, 'bob2', 20), " +
				"('BTC', 't2', 1, 'bob3', 39)," +
				"('BTC', 't2', 2, 'alice2', 1);" +
				"INSERT INTO blks VALUES ('BTC', 'b2', 2, 'b1');" +
				"INSERT INTO blks_txs (code, blk_id, tx_id) VALUES ('BTC', 'b2', 't2');" +
				"CALL new_block_updated('BTC', 0);");

			await AssertBalance(conn, "b2", "b1");

			// Replayed on different block.
			await conn.ExecuteAsync(
				"INSERT INTO blks VALUES ('BTC', 'b1-2', 1, 'b0'), ('BTC', 'b2-2', 2, 'b1-2');" +
				"INSERT INTO blks_txs (code, blk_id, tx_id) VALUES ('BTC', 'b1-2', 't1'), ('BTC', 'b2-2', 't2');" +
				"CALL new_block_updated('BTC', 0);");
			await AssertBalance(conn, "b2-2", "b1-2");

			// And again!
			await conn.ExecuteAsync(
				"INSERT INTO blks VALUES ('BTC', 'b1-3', 1, 'b0'), ('BTC', 'b2-3', 2, 'b1-3');" +
				"INSERT INTO blks_txs (code, blk_id, tx_id) VALUES ('BTC', 'b1-3', 't1'), ('BTC', 'b2-3', 't2');" +
				"CALL new_block_updated('BTC', 0);");
			await AssertBalance(conn, "b2-3", "b1-3");

			// Let's test: If the outputs are double spent, then it should disappear from the wallet balance.
			await conn.ExecuteAsync(
				"INSERT INTO txs (code, tx_id) VALUES ('BTC', 'ds'); " +
				"INSERT INTO ins VALUES ('BTC', 'ds', 0, 't1', 1); " + // This one double spend t2
				"INSERT INTO blks VALUES ('BTC', 'bs', 1, 'b0');" +
				"INSERT INTO blks_txs (code, blk_id, tx_id) VALUES ('BTC', 'bs', 'ds');" +
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
		public static async Task Orphan(this DbConnection db, string block)
		{
			var height = await db.ExecuteScalarAsync<long>("SELECT height FROM blks WHERE blk_id=@block", new { block });
			await db.ExecuteAsync("CALL orphan_blocks('BTC', @height);", new { height });
		}
	}
}
