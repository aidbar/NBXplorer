﻿using NBitcoin;
using System.Collections.Generic;

namespace NBXplorer
{
    public partial class NBXplorerNetworkProvider
    {
		public NBXplorerNetworkProvider(ChainName networkType)
		{
			NetworkType = networkType;
			InitArgoneum(networkType);
			InitBitcoin(networkType);
			InitBitcore(networkType);
			InitLitecoin(networkType);
			InitTechcoin(networkType);
			InitDogecoin(networkType);
			InitBCash(networkType);
			InitGroestlcoin(networkType);
			InitBGold(networkType);
			InitDash(networkType);
			InitTerracoin(networkType);
			InitPolis(networkType);
			InitMonacoin(networkType);
			InitFeathercoin(networkType);
			InitUfo(networkType);
			InitViacoin(networkType);
			InitMonoeci(networkType);
			InitGobyte(networkType);
			InitColossus(networkType);
			InitChaincoin(networkType);
			InitLiquid(networkType);
			InitQtum(networkType);
			InitAlthash(networkType);
			InitMonetaryUnit(networkType);
			foreach (var chain in _Networks.Values)
			{
				chain.DerivationStrategyFactory ??= chain.CreateStrategyFactory();
			}
		}

		public ChainName NetworkType
		{
			get;
			private set;
		}

		public NBXplorerNetwork GetFromCryptoCode(string cryptoCode)
		{
			_Networks.TryGetValue(cryptoCode.ToUpperInvariant(), out NBXplorerNetwork network);
			return network;
		}

		public IEnumerable<NBXplorerNetwork> GetAll()
		{
			return _Networks.Values;
		}

		Dictionary<string, NBXplorerNetwork> _Networks = new Dictionary<string, NBXplorerNetwork>();
		private void Add(NBXplorerNetwork network)
		{
			if (network.NBitcoinNetwork == null)
				return;
			_Networks.Add(network.CryptoCode, network);
		}
	}
}
