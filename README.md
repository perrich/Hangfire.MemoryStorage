Hangfire.MemoryStorage
========

A memory storage for Hangfire (http://hangfire.io).

It can be useful for testing purpose like check the behaviour and use it in a development environment.
Please note that :
* it should not be used in production (no integrity and no thread safe even if many cases are managed).
* data are stored in memory using a single static dictionary 


License:
---
Copyright 2015 - 2017 PERRICHOT Florian

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

#### How To use MemoryStorage ####

GlobalConfiguration.Configuration.UseMemoryStorage();

Example: Set a job now and execute.

Startup Class

```csharp

public void ConfigureServices(IServiceCollection services)
{   // Add This
    services.AddHangfire(config =>
    {
        config.UseMemoryStorage();
    });
}

 public void Configure(IApplicationBuilder app, IHostingEnvironment env)
{   
    // Add This
    app.UseHangfireDashboard();
    app.UseHangfireServer();
}

```

```csharp
static void Main(string[] args)
{
        
   BackgroundJob.Enqueue(() => System.IO.File.WriteAllText(@"..\test.txt","1"));
}
```


OutPut:

test.txt <br />
1


