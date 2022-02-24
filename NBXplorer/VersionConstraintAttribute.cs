using Microsoft.AspNetCore.Mvc.ActionConstraints;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace NBXplorer
{
	public enum NBXplorerVersion
	{
		V1,
		V2
	}
	public class VersionConstraintAttribute : Attribute, IActionConstraint
	{
		public VersionConstraintAttribute(NBXplorerVersion version)
		{
			Version = version;
		}

		public int Order => 100;

		public NBXplorerVersion Version { get; }

		public bool Accept(ActionConstraintContext context)
		{
			var conf = context.RouteContext.HttpContext.RequestServices.GetRequiredService<Configuration.ExplorerConfiguration>();
			return conf.UseDatabase && Version == NBXplorerVersion.V2 || !conf.UseDatabase && Version == NBXplorerVersion.V1;
		}
	}
}
