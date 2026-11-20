FROM php:8.6-cli
COPY --from=mlocati/php-extension-installer:2.13 /usr/bin/install-php-extensions /usr/local/bin/
RUN install-php-extensions intl bcmath pdo_pgsql pgsql zip redis gd opcache pcntl \
 && apt-get update && apt-get install -y --no-install-recommends git unzip procps curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=composer:2 /usr/bin/composer /usr/bin/composer
RUN groupadd -g 24128 app && useradd -u 24128 -g 24128 -m -d /home/app app
