#nullable enable
using NBXplorer.DerivationStrategy;
using System;
using System.Diagnostics.CodeAnalysis;

namespace NBXplorer
{
	public abstract class Descriptor
	{
		public static Descriptor Parse(string str, NBXplorerNetwork network)
		{
			ArgumentNullException.ThrowIfNull(str);
			var split = str.Split(':', StringSplitOptions.None);
			if (split[0] == "L")
			{
				if (split.Length != 3)
					throw new FormatException("Unexpected legacy descriptor. Format should be: (L:derivationScheme:keyPathTempalte, eg. 'L:xpub...:0/*')");
				return new LegacyDescriptor(
					network.DerivationStrategyFactory.Parse(split[1]),
					KeyPathTemplate.Parse(split[2]));
			}
			else
			{
				throw new FormatException("Unexpected descriptor format part");
			}
		}

		public abstract DerivationLine GetLineFor();
	}

	public class LegacyDescriptor : Descriptor
	{
		public LegacyDescriptor(DerivationStrategyBase derivationStrategy, KeyPathTemplate keyPathTemplate)
		{
			DerivationStrategy = derivationStrategy ?? throw new System.ArgumentNullException(nameof(derivationStrategy));
			KeyPathTemplate = keyPathTemplate ?? throw new System.ArgumentNullException(nameof(keyPathTemplate));
		}
		public KeyPathTemplate KeyPathTemplate { get; }
		public DerivationStrategyBase DerivationStrategy { get; }

		public override DerivationLine GetLineFor()
		{
			return new DerivationLine(DerivationStrategy, KeyPathTemplate);
		}

		public override string ToString()
		{
			return $"L:{DerivationStrategy}:{KeyPathTemplate}";
		}
	}
}
