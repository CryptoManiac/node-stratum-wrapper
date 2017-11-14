var redis = require('redis');
var Stratum = require('stratum-pool');
var wv = require('wallet-address-validator');

function getSubsidy(blockHeight) {
    var halvings = Math.floor(blockHeight / 210000);

    // Force block reward to zero when right shift is undefined.
    if (halvings >= 64) {
        return 0;
    }

    var nSubsidy = 5000000000;

    // Subsidy is cut in half every 210000 blocks which will occur approximately every 4 years.
    while (halvings > 0) {
      nSubsidy /= 2;
      halvings--;
    }

    return nSubsidy;
}

module.exports = function(logger, poolConfig) {

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

        var authData = shareData.worker
            .trim()
            .slice(0, 60)
            .replace(':', '');

        var [minerAddress, workerName] = authData.split('.');

        if (!wv.validate(minerAddress)) {
            minerAddress = poolConfig.defaultAddress;
            workerName = !workerName ? poolConfig.defaultName : (poolConfig.defaultName + '-' + workerName);
        }

        shareData.worker = [minerAddress, workerName].join('.');

        if (!aux){
            if (isValidShare){
                redisCommands.push(['hincrbyfloat', coin + ':shares:Today', minerAddress, shareData.difficulty]);
                redisCommands.push(['hincrby', coin + ':stats', 'validShares', 1]);

                var shareReward = getSubsidy(shareData.height) * shareData.difficulty / shareData.blockDiff;
                redisCommands.push(['hincrbyfloat', coin + ':PPS_balances', minerAddress, shareReward]);
                redisCommands.push(['hincrbyfloat', coin + ':shifts:Today', minerAddress, shareReward]);

                // Stores share diff, worker, and unique value with a score that is the timestamp. Unique value ensures it
                // doesn't overwrite an existing entry, and timestamp as score lets us query shares from last X minutes to
                // generate hashrate for each worker and pool.

                var dateNow = Date.now();
                var hashrateData = [ shareData.difficulty, shareData.worker, dateNow];
                redisCommands.push(['zadd', coin + ':hashrate', dateNow / 1000 | 0, hashrateData.join(':')]);
            }
            else{
                redisCommands.push(['hincrby', coin + ':stats', 'invalidShares', 1]);
            }
        }

        if (isValidBlock){
            redisCommands.push(['hincrby', coin + ':block_finders', minerAddress, 1]);
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
