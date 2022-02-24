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

		[HttpPost]
		[Route("cryptos/{cryptoCode}/derivations/{derivationScheme}")]
		[Route("cryptos/{cryptoCode}/addresses/{address}")]
		[VersionConstraint(NBXplorerVersion.V2)]
		public async Task<IActionResult> TrackWallet(string cryptoCode,
			[ModelBinder(BinderType = typeof(DerivationStrategyModelBinder))]
			DerivationStrategyBase derivationScheme,
			[ModelBinder(BinderType = typeof(BitcoinAddressModelBinder))]
			BitcoinAddress address, [FromBody] TrackWalletRequest request = null)
		{
			request = request ?? new TrackWalletRequest();
			TrackedSource trackedSource = GetTrackedSource(derivationScheme, address);
			if (trackedSource == null)
				return NotFound();

			var network = GetNetwork(cryptoCode, false);
			var walletId = trackedSource.GetLegacyWalletId(network);
			await using var conn = await ConnectionFactory.CreateConnectionHelper(network);
			await conn.CreateWallet(walletId);
			if (trackedSource is DerivationSchemeTrackedSource dts)
			{
				await conn.CreateDerivation(walletId, walletId);
				await conn.CreateDerivationLines(walletId, walletId,
					keyPathTemplates.GetSupportedDerivationFeatures()
					.Select(o => (o.ToString(), keyPathTemplates.GetKeyPathTemplate(o)))
					.ToArray());
				if (request.Wait)
				{
					foreach (var feature in keyPathTemplates.GetSupportedDerivationFeatures())
					{
						await conn.GenerateAddresses(walletId, walletId, feature.ToString(), GenerateAddressQuery(request, feature));
					}
				}
				else
				{
					foreach (var feature in keyPathTemplates.GetSupportedDerivationFeatures())
					{
						await conn.GenerateAddresses(walletId, walletId, feature.ToString(), new GenerateAddressQuery(minAddresses: 3, null));
					}
					foreach (var feature in keyPathTemplates.GetSupportedDerivationFeatures())
					{
						_ = conn.GenerateAddresses(walletId, walletId, feature.ToString(), GenerateAddressQuery(request, feature));
					}
				}
			}
			else if (trackedSource is IDestination ats)
			{
				await conn.AddScriptPubKeys(walletId, ats.ScriptPubKey);
			}
			return Ok();
		}

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
			changes.CurrentHeight = await conn.Connection.ExecuteScalarAsync<int>("SELECT max(height) FROM blks WHERE code=@code AND confirmed = 't'", new { code = network.CryptoCode });
			foreach (var row in await conn.Connection.QueryAsync<(string tx_id, int idx, string scriptpubkey, long value, long height, string keypath, string line_name, DateTime created_at)>
				("SELECT u.tx_id, u.idx, u.scriptpubkey, u.value, u.height, ts.keypath, ts.line_name, u.created_at " +
				"FROM conf_utxos u " +
				"INNER JOIN tracked_scriptpubkeys ts ON u.code=ts.code AND u.scriptpubkey=ts.scriptpubkey " +
				"WHERE u.code=@code AND u.wallet_id=@walletId", new { code = network.CryptoCode, walletId = trackedSource.GetLegacyWalletId(network) }))
			{
				var txid = uint256.Parse(row.tx_id);
				changes.Confirmed.UTXOs.Add(new UTXO()
				{
					Confirmations = (int)(changes.CurrentHeight - row.height + 1),
					Index = row.idx,
					Outpoint = new OutPoint(txid, row.idx),
					KeyPath = KeyPath.Parse(row.keypath),
					ScriptPubKey = Script.FromBytesUnsafe(Encoders.Hex.DecodeData(row.scriptpubkey)),
					Timestamp = new DateTimeOffset(row.created_at),
					TransactionHash = txid,
					Value = Money.Satoshis(row.value),
					Feature = Enum.Parse<DerivationFeature>(row.line_name)
				});
			}
			//var chain = ChainProvider.GetChain(network);
			var repo = RepositoryProvider.GetRepository(network);

			//changes = new UTXOChanges();
			//changes.CurrentHeight = chain.Height;
			//var transactions = await GetAnnotatedTransactions(repo, chain, trackedSource);

			//changes.Confirmed = ToUTXOChange(transactions.ConfirmedState);
			//changes.Confirmed.SpentOutpoints.Clear();
			//changes.Unconfirmed = ToUTXOChange(transactions.UnconfirmedState - transactions.ConfirmedState);



			//FillUTXOsInformation(changes.Confirmed.UTXOs, transactions, changes.CurrentHeight);
			//FillUTXOsInformation(changes.Unconfirmed.UTXOs, transactions, changes.CurrentHeight);

			//changes.TrackedSource = trackedSource;
			//changes.DerivationStrategy = (trackedSource as DerivationSchemeTrackedSource)?.DerivationStrategy;
			//changes.Confirmed.
			//return Json(changes, repo.Serializer.Settings);
			return Json(changes, repo.Serializer.Settings);
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
	}
}
