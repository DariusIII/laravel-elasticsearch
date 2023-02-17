<?php

namespace Cviebrock\LaravelElasticsearch;

use Elastic\Elasticsearch\Client;
use Elastic\Elasticsearch\ClientBuilder;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Uri;
use Guzzle\RingPHP\Future\CompletedFutureArray;
use Illuminate\Support\Arr;
use Illuminate\Support\Reflector;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;


class Factory
{

    /**
     * Map configuration array keys with ES ClientBuilder setters
     *
     * @var array
     */
    protected $configMappings = [
        'sslVerification'    => 'setSSLVerification',
        'retries'            => 'setRetries',
        'nodePool'           => 'setNodePool',
        'serializer'         => 'setSerializer',
    ];

    /**
     * Make the Elasticsearch client for the given named configuration, or
     * the default client.
     *
     * @param array $config
     *
     * @return \Elastic\Elasticsearch\Client
     * @throws \Elastic\Elasticsearch\Exception\AuthenticationException
     */
    public function make(array $config): Client
    {
        return $this->buildClient($config);
    }

    /**
     * Build and configure an Elasticsearch client.
     *
     * @param array $config
     *
     * @return \Elastic\Elasticsearch\Client
     * @throws \Elastic\Elasticsearch\Exception\AuthenticationException
     */
    protected function buildClient(array $config): Client
    {
        $clientBuilder = ClientBuilder::create();

        // Configure hosts
        $clientBuilder->setHosts($config['hosts']);
        $clientBuilder->setBasicAuthentication($config['basicAuthentication']['user'], $config['basicAuthentication']['pass']);

        // Configure logging
        if (Arr::get($config, 'logging')) {
            $logObject = Arr::get($config, 'logObject');
            $logPath = Arr::get($config, 'logPath');
            $logLevel = Arr::get($config, 'logLevel');
            if ($logObject instanceof LoggerInterface) {
                $clientBuilder->setLogger($logObject);
            } elseif ($logPath && $logLevel) {
                $handler = new StreamHandler($logPath, $logLevel);
                $logObject = new Logger('log');
                $logObject->pushHandler($handler);
                $clientBuilder->setLogger($logObject);
            }
        }

        // Set additional client configuration
        foreach ($this->configMappings as $key => $method) {
            $value = Arr::get($config, $key);
            if (is_array($value)) {
                foreach ($value as $vItem) {
                    $clientBuilder->$method($vItem);
                }
            } elseif ($value !== null) {
                $clientBuilder->$method($value);
            }
        }

        // Build and return the client
        if (!empty($config['cloud_api']['api_id']) && !empty($config['cloud_api']['api_key'])
        ) {
            $clientBuilder->setElasticCloudId($config['cloud_api']['api_id'])->setApiKey($config['cloud_api']['api_key']);
        }

        // Build and return the client
        return $clientBuilder->build();
    }
}
