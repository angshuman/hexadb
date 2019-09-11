using Microsoft.AspNetCore.Http;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Hexastore.Web
{
    public class PerfHeaderMiddleware
    {
        private readonly RequestDelegate _next;

        public PerfHeaderMiddleware(RequestDelegate next)
        {
            _next = next;
        }

        public async Task InvokeAsync(HttpContext context)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            context.Response.OnStarting(() =>
            {
                stopwatch.Stop();
                context.Response.Headers.Add("x-response-time", stopwatch.ElapsedMilliseconds.ToString());
                context.Response.Headers.Add("x-cpus", Environment.ProcessorCount.ToString());
                context.Response.Headers.Add("x-working-set", (Environment.WorkingSet / 1000_000).ToString());
                return Task.CompletedTask;
            });
            await _next(context);
        }
    }
}