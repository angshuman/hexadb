#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app
EXPOSE 5000
EXPOSE 5001

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["NuGet.Config", "."]
COPY ["Hexastore.GRPC/Hexastore.GRPC.csproj", "Hexastore.GRPC/"]
COPY ["Hexastore/Hexastore.csproj", "Hexastore/"]
COPY ["Hexastore.Rocks/Hexastore.Rocks.csproj", "Hexastore.Rocks/"]
RUN dotnet dev-certs https
RUN dotnet restore "Hexastore.GRPC/Hexastore.GRPC.csproj"
COPY . .
WORKDIR "/src/Hexastore.GRPC"
RUN dotnet build "Hexastore.GRPC.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet dev-certs https 
RUN dotnet publish "Hexastore.GRPC.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
RUN apt-get update && apt-get install -y libcap2-bin libsnappy1v5 && \
    ln -s /lib/x86_64-linux-gnu/libdl.so.2 /usr/lib/x86_64-linux-gnu/libdl.so && \
    ln -s /lib/x86_64-linux-gnu/libc.so.6 /usr/lib/x86_64-linux-gnu/libc.so && \
    rm -rf /var/lib/apt/lists/*
COPY --from=publish /app/publish .
ENV ENV LD_LIBRARY_PATH=ENV LD_LIBRARY_PATH:/app/bin/Release/net6.0/runtimes/linux-x64/native:/app/bin/Release/net6.0/runtimes/linux-x64/native
ENTRYPOINT ["dotnet", "Hexastore.GRPC.dll"]