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
			KeyPathTemplates keyPathTemplates) : base(networkProvider, waiters)
		{
			ConnectionFactory = connectionFactory;
			KeyPathTemplates = keyPathTemplates;
		}

		public DbConnectionFactory ConnectionFactory { get; }
		public KeyPathTemplates KeyPathTemplates { get; }

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
			var b = await conn.QueryFirstOrDefaultAsync("SELECT * FROM wallets_balances WHERE code=@code AND wallet_id=@walletId", new { code = network.CryptoCode, walletId = trackedSource.GetLegacyWalletId(network) });
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
			await using var conn = await ConnectionFactory.CreateConnection();
			var height = await conn.ExecuteScalarAsync<long>("SELECT height FROM get_tip(@code)", new { code = network.CryptoCode });
			string join = derivationScheme is null ? string.Empty : " JOIN descriptors_scripts USING (code, script)";
			string column = derivationScheme is null ? "NULL as keypath" : " keypath";
			var utxos = (await conn.QueryAsync<(
				long? height,
				string tx_id,
				int idx,
				long value,
				string script,
				string keypath,
				bool mempool,
				bool spent_mempool,
				DateTime tx_seen_at)>($"SELECT height, tx_id, wu.idx, value, script, {column}, mempool, spent_mempool, seen_at FROM wallets_utxos wu {join} WHERE code=@code AND wallet_id=@walletId AND immature IS FALSE", new { code = network.CryptoCode, walletId = trackedSource.GetLegacyWalletId(network) }));
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
				if (utxo.height is long)
				{
					u.Confirmations = (int)(height - utxo.height + 1);
				}
				if (utxo.keypath is not null)
				{
					u.KeyPath = KeyPath.Parse(utxo.keypath);
					u.Feature = KeyPathTemplates.GetDerivationFeature(u.KeyPath);
				}
				if (!utxo.mempool)
					changes.Confirmed.UTXOs.Add(u);
				else if (!utxo.spent_mempool)
						changes.Unconfirmed.UTXOs.Add(u);
				if (utxo.spent_mempool && !utxo.mempool)
					changes.Unconfirmed.SpentOutpoints.Add(u.Outpoint);
			}
			return Json(changes, network.JsonSerializerSettings);
		}
	}
}
