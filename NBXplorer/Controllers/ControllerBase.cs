using Microsoft.AspNetCore.Mvc;
using NBitcoin;
using NBXplorer.DerivationStrategy;
using NBXplorer.Models;
using System;

namespace NBXplorer.Controllers
{
	public class ControllerBase : Controller
	{
		public ControllerBase(
			NBXplorerNetworkProvider networkProvider,
			BitcoinDWaiters waiters)
		{
			NetworkProvider = networkProvider;
			Waiters = waiters;
		}

		public NBXplorerNetworkProvider NetworkProvider { get; }
		public BitcoinDWaiters Waiters { get; }

		internal static TrackedSource GetTrackedSource(DerivationStrategyBase derivationScheme, BitcoinAddress address)
		{
			TrackedSource trackedSource = null;
			if (address != null)
				trackedSource = new AddressTrackedSource(address);
			if (derivationScheme != null)
				trackedSource = new DerivationSchemeTrackedSource(derivationScheme);
			return trackedSource;
		}
		internal NBXplorerNetwork GetNetwork(string cryptoCode, bool checkRPC)
		{
			if (cryptoCode == null)
				throw new ArgumentNullException(nameof(cryptoCode));
			cryptoCode = cryptoCode.ToUpperInvariant();
			var network = NetworkProvider.GetFromCryptoCode(cryptoCode);
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
