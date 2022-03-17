using Dapper;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using NBitcoin;
using NBXplorer.DerivationStrategy;
using NBXplorer.ModelBinders;
using NBXplorer.Models;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace NBXplorer.Controllers
{
	[Route("v1")]
	[Authorize]
	public class PostgresMainController : ControllerBase
	{
		public PostgresMainController(
			DbConnectionFactory connectionFactory,
			NBXplorerNetworkProvider networkProvider,
			BitcoinDWaiters waiters,
			KeyPathTemplates keyPathTemplates,
			IRepositoryProvider repositoryProvider) : base(networkProvider, waiters)
		{
			ConnectionFactory = connectionFactory;
			KeyPathTemplates = keyPathTemplates;
			RepositoryProvider = repositoryProvider;
		}

		public DbConnectionFactory ConnectionFactory { get; }
		public KeyPathTemplates KeyPathTemplates { get; }
		public IRepositoryProvider RepositoryProvider { get; }

		[HttpGet]
		[Route("cryptos/{cryptoCode}/derivations/{derivationScheme}/balance")]
		[Route("cryptos/{cryptoCode}/addresses/{address}/balance")]
		[PostgresImplementationActionConstraint(true)]
		public async Task<IActionResult> GetBalance(string cryptoCode,
			[ModelBinder(BinderType = typeof(DerivationStrategyModelBinder))]
			DerivationStrategyBase derivationScheme,
			[ModelBinder(BinderType = typeof(BitcoinAddressModelBinder))]
			BitcoinAddress address)
		{
			var trackedSource = GetTrackedSource(derivationScheme, address);
			if (trackedSource == null)
				throw new ArgumentNullException(nameof(trackedSource));
			var network = GetNetwork(cryptoCode, false);
			var repo = (PostgresRepository)RepositoryProvider.GetRepository(cryptoCode);
			await using var conn = await ConnectionFactory.CreateConnection();
			var b = await conn.QueryFirstOrDefaultAsync("SELECT * FROM wallets_balances WHERE code=@code AND wallet_id=@walletId", new { code = network.CryptoCode, walletId = repo.GetWalletKey(trackedSource).wid });
			if (b is null)
			{
				return Json(new GetBalanceResponse()
				{
					Available = Money.Zero,
					Confirmed = Money.Zero,
					Immature = Money.Zero,
					Total = Money.Zero,
					Unconfirmed = Money.Zero
				}, network.JsonSerializerSettings);
			}
			var balance = new GetBalanceResponse()
			{
				Confirmed = Money.Satoshis((long)b.confirmed_balance),
				Unconfirmed = Money.Satoshis((long)b.unconfirmed_balance - b.confirmed_balance),
				Available = Money.Satoshis((long)b.available_balance),
				Total = Money.Satoshis((long)b.available_balance + b.immature_balance),
				Immature = Money.Satoshis((long)b.immature_balance)
			};
			balance.Total = balance.Confirmed.Add(balance.Unconfirmed);
			return Json(balance, network.JsonSerializerSettings);
		}

		[HttpGet]
		[Route("cryptos/{cryptoCode}/derivations/{derivationScheme}/utxos")]
		[Route("cryptos/{cryptoCode}/addresses/{address}/utxos")]
		[PostgresImplementationActionConstraint(true)]
		public async Task<IActionResult> GetUTXOs(
			string cryptoCode,
			[ModelBinder(BinderType = typeof(DerivationStrategyModelBinder))]
			DerivationStrategyBase derivationScheme,
			[ModelBinder(BinderType = typeof(BitcoinAddressModelBinder))]
			BitcoinAddress address)
		{
			var trackedSource = GetTrackedSource(derivationScheme, address);
			if (trackedSource == null)
				throw new ArgumentNullException(nameof(trackedSource));
			var network = GetNetwork(cryptoCode, false);
			var repo = (PostgresRepository)RepositoryProvider.GetRepository(cryptoCode);

			await using var conn = await ConnectionFactory.CreateConnection();
			var height = await conn.ExecuteScalarAsync<long>("SELECT height FROM get_tip(@code)", new { code = network.CryptoCode });
			string join = derivationScheme is null ? string.Empty : " JOIN descriptors_scripts ds USING (code, script) JOIN descriptors d USING (code, descriptor)";
			string column = derivationScheme is null ? "NULL as keypath, NULL as feature" : "nbxv1_get_keypath(d.metadata, ds.idx) AS keypath, d.metadata->>'feature' feature";
			var utxos = (await conn.QueryAsync<(
				long? blk_height,
				string tx_id,
				int idx,
				long value,
				string script,
				string keypath,
				string feature,
				bool mempool,
				bool input_mempool,
				DateTime tx_seen_at)>(
				$"SELECT blk_height, tx_id, wu.idx, value, script, {column}, mempool, input_mempool, seen_at " +
				$"FROM wallets_utxos wu {join} WHERE code=@code AND wallet_id=@walletId AND immature IS FALSE", new { code = network.CryptoCode, walletId = repo.GetWalletKey(trackedSource).wid }));
			UTXOChanges changes = new UTXOChanges()
			{
				CurrentHeight = (int)height,
				TrackedSource = trackedSource,
				DerivationStrategy = derivationScheme
			};
			foreach (var utxo in utxos.OrderBy(u => u.tx_seen_at))
			{
				var u = new UTXO()
				{
					Index = utxo.idx,
					Timestamp = new DateTimeOffset(utxo.tx_seen_at),
					Value = Money.Satoshis(utxo.value),
					ScriptPubKey = Script.FromHex(utxo.script),
					TransactionHash = uint256.Parse(utxo.tx_id)
				};
				u.Outpoint = new OutPoint(u.TransactionHash, u.Index);
				if (utxo.blk_height is long)
				{
					u.Confirmations = (int)(height - utxo.blk_height + 1);
				}

				if (utxo.keypath is not null)
				{
					u.KeyPath = KeyPath.Parse(utxo.keypath);
					u.Feature = Enum.Parse<DerivationFeature>(utxo.feature);
				}
				if (!utxo.mempool)
					changes.Confirmed.UTXOs.Add(u);
				else if (!utxo.input_mempool)
						changes.Unconfirmed.UTXOs.Add(u);
				if (utxo.input_mempool && !utxo.mempool)
					changes.Unconfirmed.SpentOutpoints.Add(u.Outpoint);
			}
			return Json(changes, network.JsonSerializerSettings);
		}
	}
}
