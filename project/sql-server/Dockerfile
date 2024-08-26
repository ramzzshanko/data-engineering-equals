FROM mcr.microsoft.com/mssql/server:2019-latest

# Switch to root to install packages
USER root

# Install necessary packages
RUN apt-get update && apt-get install -y \
    curl \
    apt-transport-https \
    gnupg

# Add Microsoft's GPG key
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

# Add Microsoft SQL Server repository
RUN curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Install SQL Server tools
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y mssql-tools unixodbc-dev

# Add SQL Server tools to PATH
ENV PATH="/opt/mssql-tools/bin:${PATH}"

# Switch back to mssql user
USER mssql

# Copy initialization script
COPY create_database.sql ./

# Set environment variables
ENV ACCEPT_EULA=Y
ENV SA_PASSWORD=Password2024!

RUN (/opt/mssql/bin/sqlservr --accept-eula & ) | grep -q "Service Broker manager has started" \
&& /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P $SA_PASSWORD -i create_database.sql

# Expose SQL Server port
EXPOSE 1433

# Start SQL Server
CMD ["/opt/mssql/bin/sqlservr"]