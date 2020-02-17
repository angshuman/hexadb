FROM mcr.microsoft.com/dotnet/core/sdk:3.1 AS build-env
WORKDIR /app

# Copy csproj and restore as distinct layers
COPY ./Hexastore.Web/Hexastore.Web.csproj ./Hexastore.Web
COPY ./Hexastore/Hexastore.csproj ./Hexastore
COPY ./Hexastore.Rocks/Hexastore.Rocks.csproj ./Hexastore.Rocks.csproj

RUN dotnet restore 

COPY . ./

RUN apt-get update && apt-get install -y libcap2-bin libsnappy1v5 && \
    ln -s /lib/x86_64-linux-gnu/libdl.so.2 /usr/lib/x86_64-linux-gnu/libdl.so && \
    ln -s /lib/x86_64-linux-gnu/libc.so.6 /usr/lib/x86_64-linux-gnu/libc.so && \
    rm -rf /var/lib/apt/lists/*

RUN dotnet test Hexastore.Test

# Copy everything else and build
RUN dotnet publish -c Release ./Hexastore.Web -o /app/out

# Build runtime image
FROM mcr.microsoft.com/dotnet/core/aspnet:3.1
WORKDIR /app
COPY --from=build-env /app/out .

RUN apt-get update && apt-get install -y libcap2-bin libsnappy1v5 && \
    ln -s /lib/x86_64-linux-gnu/libdl.so.2 /usr/lib/x86_64-linux-gnu/libdl.so && \
    ln -s /lib/x86_64-linux-gnu/libc.so.6 /usr/lib/x86_64-linux-gnu/libc.so && \
    rm -rf /var/lib/apt/lists/*

ENV ENV LD_LIBRARY_PATH=ENV LD_LIBRARY_PATH:/app/bin/Debug/netcoreapp3.1/native/amd64/:/app/bin/Release/netcoreapp3.1/native/amd64/
EXPOSE 80
ENTRYPOINT ["dotnet", "Hexastore.Web.dll"]
