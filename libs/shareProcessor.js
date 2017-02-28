var redis = require('redis');
var Stratum = require('stratum-pool');
var wv = require('wallet-address-validator');

/*
This module deals with handling shares when in internal payment processing mode. It connects to a redis
database and inserts shares with the database structure of:

key: coin_name + ':' + block_height
value: a hash with..
        key:

 */



module.exports = function(logger, poolConfig){

    var redisConfig = poolConfig.redis;
    var coin = poolConfig.coin.name;
    var coinSymbol=poolConfig.coin.symbol;

    var forkId = process.env.forkId;
    var logSystem = 'Pool';
    var logComponent = coin;
    var logSubCat = 'Thread ' + (parseInt(forkId) + 1);

    var connection = redis.createClient(redisConfig.port, redisConfig.host);
    // redis auth if needed
     connection.auth(redisConfig.password);
     connection.select(redisConfig.db);

    connection.on('ready', function(){
        logger.debug(logSystem, logComponent, logSubCat, 'Share processing setup with redis (' + redisConfig.host +
            ':' + redisConfig.port  + ')');
    });
    connection.on('error', function(err){
        logger.error(logSystem, logComponent, logSubCat, 'Redis client had an error: ' + JSON.stringify(err))
    });
    connection.on('end', function(){
        logger.error(logSystem, logComponent, logSubCat, 'Connection to redis database as been ended');
    });

    connection.info(function(error, response){
        if (error){
            logger.error(logSystem, logComponent, logSubCat, 'Redis version check failed');
            return;
        }
        var parts = response.split('\r\n');
        var version;
        var versionString;
        for (var i = 0; i < parts.length; i++){
            if (parts[i].indexOf(':') !== -1){
                var valParts = parts[i].split(':');
                if (valParts[0] === 'redis_version'){
                    versionString = valParts[1];
                    version = parseFloat(versionString);
                    break;
                }
            }
        }
        if (!version){
            logger.error(logSystem, logComponent, logSubCat, 'Could not detect redis version - but be super old or broken');
        }
        else if (version < 2.6){
            logger.error(logSystem, logComponent, logSubCat, "You're using redis version " + versionString + " the minimum required version is 2.6. Follow the damn usage instructions...");
        }
    });


    this.handleAuxBlock = function(isValidBlock, height, hash, tx, diff, coin){

        var redisCommands = [];

        if (isValidBlock){
            redisCommands.push(['hincrby', coin + ':stats', 'validBlocks', 1]);
        }
        else if (hash){
            redisCommands.push(['hincrby', coin + ':stats', 'invalidBlocks', 1]);
        }

        connection.multi(redisCommands).exec(function(err, replies){
            if (err)
                logger.error(logSystem, logComponent, logSubCat, 'Error with share processor multi ' + JSON.stringify(err));
        });

    };

    this.handleShare = function(isValidShare, isValidBlock, shareData, coin, aux){
        var redisCommands = [];
        shareData.worker = shareData.worker.trim();

        var minerAddress = shareData.worker.split('.')[0];
        if (!wv.validate(minerAddress)) {
            shareData.worker = poolConfig.address;
        } else {
            shareData.worker = shareData.worker.slice(0, 60).replace(':', '');
        }

        if (isValidShare){
            redisCommands.push(['hincrbyfloat', coin + ':shares:Today', shareData.worker.split('.')[0], shareData.difficulty]);
            redisCommands.push(['hincrby', coin + ':stats', 'validShares', 1]);

            var blockReward = 12.5 * 100000000;
            var shareReward = (blockReward * shareData.difficulty) / shareData.blockDiff;
            redisCommands.push(['hincrbyfloat', coin + ':PPS_balances', shareData.worker.split('.')[0], shareReward]);
        }
        else{
            redisCommands.push(['hincrby', coin + ':stats', 'invalidShares', 1]);
        }
        /* Stores share diff, worker, and unique value with a score that is the timestamp. Unique value ensures it
           doesn't overwrite an existing entry, and timestamp as score lets us query shares from last X minutes to
           generate hashrate for each worker and pool. */
        var dateNow = Date.now();
        if (aux != true){
            var hashrateData = [ isValidShare ? shareData.difficulty : -shareData.difficulty, shareData.worker, dateNow];
            redisCommands.push(['zadd', coin + ':hashrate', dateNow / 1000 | 0, hashrateData.join(':')]);
        }
        if (isValidBlock){
            redisCommands.push(['hincrby', coin + ':block_finders', shareData.worker.split('.')[0], 1]);
            redisCommands.push(['hincrby', coin + ':stats', 'validBlocks', 1]);
        }
        else if (shareData.blockHash){
            redisCommands.push(['hincrby', coin + ':stats', 'invalidBlocks', 1]);
        }

        connection.multi(redisCommands).exec(function(err, replies){
            if (err)
                logger.error(logSystem, logComponent, logSubCat, 'Error with share processor multi ' + JSON.stringify(err));
        });

    };

};