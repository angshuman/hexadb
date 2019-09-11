FROM microsoft/dotnet:2.2-aspnetcore-runtime AS base
RUN apt-get update && apt-get install -y libcap2-bin libsnappy1v5 && \
    ln -s /lib/x86_64-linux-gnu/libdl.so.2 /usr/lib/x86_64-linux-gnu/libdl.so && \
    ln -s /lib/x86_64-linux-gnu/libc.so.6 /usr/lib/x86_64-linux-gnu/libc.so && \
    rm -rf /var/lib/apt/lists/*

ENV ENV LD_LIBRARY_PATH=ENV LD_LIBRARY_PATH:/app/bin/Debug/netcoreapp2.2/native/amd64/:/app/bin/Release/netcoreapp2.2/native/amd64/
WORKDIR /app
EXPOSE 80

FROM microsoft/dotnet:2.2-sdk AS build
WORKDIR /src
COPY ["Hexastore.Web/Hexastore.Web.csproj", "Hexastore.Web/"]
RUN dotnet restore "Hexastore.Web/Hexastore.Web.csproj"
COPY . .
WORKDIR "/src/Hexastore.Web"
RUN dotnet build "Hexastore.Web.csproj" -c Release -o /app

FROM build AS publish
RUN dotnet publish "Hexastore.Web.csproj" -c Release -o /app

FROM base AS final
WORKDIR /app
COPY --from=publish /app .
COPY Hexastore.env /var/data
RUN apt-get update && apt-get install -y libcap2-bin libsnappy1v5 && \
    ln -s /lib/x86_64-linux-gnu/libdl.so.2 /usr/lib/x86_64-linux-gnu/libdl.so && \
    ln -s /lib/x86_64-linux-gnu/libc.so.6 /usr/lib/x86_64-linux-gnu/libc.so && \
    rm -rf /var/lib/apt/lists/*

ENV ENV LD_LIBRARY_PATH=ENV LD_LIBRARY_PATH:/app/bin/Debug/netcoreapp2.2/native/amd64/:/app/bin/Release/netcoreapp2.2/native/amd64/
ENTRYPOINT ["dotnet", "Hexastore.Web.dll"]