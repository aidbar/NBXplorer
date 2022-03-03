using Dapper;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using NBitcoin;
using NBitcoin.DataEncoders;
using NBXplorer.DerivationStrategy;
using NBXplorer.ModelBinders;
using NBXplorer.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NBXplorer.Controllers
{
	[Route("v1")]
	[Authorize]
	// The controller attempt to keep NBXplorer v1 API working on the postgres backend
	public class MainV2LegacyController : Controller
	{
		public MainV2LegacyController(
			BitcoinDWaiters waiters,
			KeyPathTemplates keyPathTemplates,
			IRepositoryProvider repositoryProvider,
			DbConnectionFactory connectionFactory)
		{
			Waiters = waiters;
			ConnectionFactory = connectionFactory;
			this.keyPathTemplates = keyPathTemplates;
			RepositoryProvider = repositoryProvider;
		}
		public BitcoinDWaiters Waiters
		{
			get; set;
		}
		public IRepositoryProvider RepositoryProvider { get; }

		private readonly KeyPathTemplates keyPathTemplates;
		public DbConnectionFactory ConnectionFactory { get; }

		[HttpGet]
		[Route("cryptos/{cryptoCode}/derivations/{derivationScheme}/utxos")]
		[Route("cryptos/{cryptoCode}/addresses/{address}/utxos")]
		[VersionConstraint(NBXplorerVersion.V2)]
		public async Task<IActionResult> GetUTXOs(
			string cryptoCode,
			[ModelBinder(BinderType = typeof(DerivationStrategyModelBinder))]
			DerivationStrategyBase derivationScheme,
			[ModelBinder(BinderType = typeof(BitcoinAddressModelBinder))]
			BitcoinAddress address)
		{
			var trackedSource = GetTrackedSource(derivationScheme, address);
			UTXOChanges changes = new UTXOChanges();
			if (trackedSource == null)
				throw new ArgumentNullException(nameof(trackedSource));
			var network = GetNetwork(cryptoCode, false);
			await using var conn = await ConnectionFactory.CreateConnectionHelper(network);
			changes.CurrentHeight = (await conn.GetTip()).Height;
			changes.DerivationStrategy = derivationScheme;
			changes.TrackedSource = trackedSource;
			var walletId = trackedSource.GetLegacyWalletId(network);
			changes.Confirmed.UTXOs = await conn.GetWalletUTXOWithDescriptors(network, walletId, changes.CurrentHeight);
			Dictionary<OutPoint, UTXO> outputsByOutpoint = new Dictionary<OutPoint, UTXO>((int)(changes.Confirmed.UTXOs.Count * 1.1));
			foreach (var utxo in changes.Confirmed.UTXOs)
			{
				outputsByOutpoint.Add(utxo.Outpoint, utxo);
			}

			var spentOutpoints = await conn.Connection.QueryAsync<(string wallet_id, string out_tx_id, int idx, string source, long value, string script, string keypath, DateTime seen_at)>(
				"SELECT ts.wallet_id, io.out_tx_id, io.idx, io.source, io.value, io.script, ts.keypath, io.seen_at FROM unconf_ins_outs io " +
				"LEFT JOIN tracked_scripts ts USING (code, script) " +
				"WHERE code=@code AND wallet_id=@walletId " +
				"ORDER BY io.seen_at", new { code = network.CryptoCode, walletId = walletId });
			foreach (var row in spentOutpoints)
			{
				var outpoint = new OutPoint(uint256.Parse(row.out_tx_id), row.idx);
				var keypath = row.keypath is null ? null : KeyPath.Parse(row.keypath);
				if (row.source == "INPUT")
				{
					if (!outputsByOutpoint.Remove(outpoint))
						// Conflict, must be double spend
						continue;
					if (changes.Unconfirmed.UTXOs.RemoveAll(u => u.Outpoint == outpoint) == 0)
						changes.Unconfirmed.SpentOutpoints.Add(outpoint);
				}
				else
				{
					var utxo = new UTXO()
					{
						Confirmations = 0,
						Index = row.idx,
						Outpoint = outpoint,
						KeyPath = keypath,
						ScriptPubKey = Script.FromHex(row.script),
						Timestamp = new DateTimeOffset(row.seen_at),
						TransactionHash = outpoint.Hash,
						Value = Money.Satoshis(row.value),
						Feature = keypath is null ? null : keyPathTemplates.GetDerivationFeature(keypath)
					};
					outputsByOutpoint.Add(utxo.Outpoint, utxo);
					changes.Unconfirmed.UTXOs.Add(utxo);
				}
			}
			return Json(changes, network.Serializer.Settings);
		}

		private GenerateAddressQuery GenerateAddressQuery(TrackWalletRequest request, DerivationFeature feature)
		{
			if (request?.DerivationOptions == null)
				return null;
			foreach (var derivationOption in request.DerivationOptions)
			{
				if ((derivationOption.Feature is DerivationFeature f && f == feature) || derivationOption.Feature is null)
				{
					return new GenerateAddressQuery(derivationOption.MinAddresses, derivationOption.MaxAddresses);
				}
			}
			return null;
		}

		private static TrackedSource GetTrackedSource(DerivationStrategyBase derivationScheme, BitcoinAddress address)
		{
			TrackedSource trackedSource = null;
			if (address != null)
				trackedSource = new AddressTrackedSource(address);
			if (derivationScheme != null)
				trackedSource = new DerivationSchemeTrackedSource(derivationScheme);
			return trackedSource;
		}
		private NBXplorerNetwork GetNetwork(string cryptoCode, bool checkRPC)
		{
			if (cryptoCode == null)
				throw new ArgumentNullException(nameof(cryptoCode));
			cryptoCode = cryptoCode.ToUpperInvariant();
			var network = Waiters.GetWaiter(cryptoCode)?.Network;
			if (network == null)
				throw new NBXplorerException(new NBXplorerError(404, "cryptoCode-not-supported", $"{cryptoCode} is not supported"));

			if (checkRPC)
			{
				var waiter = Waiters.GetWaiter(network);
				if (waiter == null || !waiter.RPCAvailable || waiter.RPC.Capabilities == null)
					throw new NBXplorerError(400, "rpc-unavailable", $"The RPC interface is currently not available.").AsException();
			}
			return network;
		}


		[HttpGet]
		[Route("cryptos/{cryptoCode}/derivations/{derivationScheme}/balance")]
		[Route("cryptos/{cryptoCode}/addresses/{address}/balance")]
		[VersionConstraint(NBXplorerVersion.V2)]
		public async Task<IActionResult> GetBalance(string cryptoCode,
		[ModelBinder(BinderType = typeof(DerivationStrategyModelBinder))]
			DerivationStrategyBase derivationScheme,
		[ModelBinder(BinderType = typeof(BitcoinAddressModelBinder))]
			BitcoinAddress address)
		{
			var trackedSource = GetTrackedSource(derivationScheme, address);
			UTXOChanges changes = new UTXOChanges();
			if (trackedSource == null)
				throw new ArgumentNullException(nameof(trackedSource));
			var network = GetNetwork(cryptoCode, false);

			var walletId = trackedSource.GetLegacyWalletId(network);
			await using var conn = await ConnectionFactory.CreateConnectionHelper(network);

			var balance = new GetBalanceResponse();
			var b = await conn.Connection.QueryFirstAsync<(long conf_balance, long immature_balance)>(
				"SELECT COALESCE(SUM(value), 0) conf_balance," +
				"       COALESCE(SUM (value) FILTER (WHERE immature='t'), 0) immature_balance " +
				"FROM get_wallet_conf_utxos(@code, @walletId)", new { code = network.CryptoCode, walletId });

			balance.Confirmed = Money.Satoshis(b.conf_balance);
			balance.Unconfirmed = Money.Zero;
			balance.Immature = Money.Satoshis(b.immature_balance);
			balance.Total = balance.Confirmed.Add(balance.Unconfirmed);
			balance.Available = balance.Total.Sub(balance.Immature);
			return Json(balance);
		}
	}
}
