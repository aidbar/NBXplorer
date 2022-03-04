using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NBXplorer.Configuration;
using Npgsql;
using System.Data.Common;
using System.Threading.Tasks;

namespace NBXplorer
{
	public class DbConnectionFactory
	{
		public DbConnectionFactory(ILogger<DbConnectionFactory> logger,
			IConfiguration configuration,
			ExplorerConfiguration conf,
			KeyPathTemplates keyPathTemplates)
		{
			Logger = logger;
			ExplorerConfiguration = conf;
			KeyPathTemplates = keyPathTemplates;
			ConnectionString = configuration.GetRequired("POSTGRES");
		}

		public string ConnectionString { get; }
		public ILogger<DbConnectionFactory> Logger { get; }
		public ExplorerConfiguration ExplorerConfiguration { get; }
		public KeyPathTemplates KeyPathTemplates { get; }

		public async Task<DbConnectionHelper> CreateConnectionHelper(NBXplorerNetwork network)
		{
			return new DbConnectionHelper(network, await CreateConnection(), KeyPathTemplates)
			{
				MinPoolSize = ExplorerConfiguration.MinGapSize,
				MaxPoolSize = ExplorerConfiguration.MaxGapSize
			};
		}
		public async Task<DbConnection> CreateConnection()
		{
			int maxRetries = 10;
			int retries = maxRetries;
			retry:
			var conn = new Npgsql.NpgsqlConnection(ConnectionString);
			try
			{
				await conn.OpenAsync();
			}
			catch (PostgresException ex) when (ex.IsTransient && retries > 0)
			{
				retries--;
				await conn.DisposeAsync();
				await Task.Delay((maxRetries - retries) * 100);
				goto retry;
			}
			catch
			{
				conn.Dispose();
				throw;
			}
			return conn;
		}
	}
}
