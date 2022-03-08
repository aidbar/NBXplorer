using Dapper;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using NBitcoin;
using NBXplorer.DerivationStrategy;
using NBXplorer.ModelBinders;
using NBXplorer.Models;
using System;
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
			BitcoinDWaiters waiters) : base(networkProvider, waiters)
		{
			ConnectionFactory = connectionFactory;
		}

		public DbConnectionFactory ConnectionFactory { get; }

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

			await using var conn = await ConnectionFactory.CreateConnection();
			var b = await conn.QueryFirstOrDefaultAsync("SELECT * FROM wallets_balances WHERE wallet_id=@walletId", new { walletId = trackedSource.GetLegacyWalletId(network) });
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
	}
}
