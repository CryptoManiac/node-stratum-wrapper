var Stratum = require('stratum-pool');
var redis   = require('redis');
var net     = require('net');

var ShareProcessor = require('./shareProcessor.js');

module.exports = function(logger){

    var _this = this;

    var poolConfigs  = JSON.parse(process.env.pools);
    var portalConfig = JSON.parse(process.env.portalConfig);
    var forkId = process.env.forkId;
    var pools = {};
    var proxySwitch = {};

    var redisClient = redis.createClient(portalConfig.redis.port, portalConfig.redis.host);

    // redis auth if enabled 
    redisClient.auth(portalConfig.redis.password);
    redisClient.select(portalConfig.redis.db);

    //Handle messages from master process sent via IPC
    process.on('message', function(message) {
        switch(message.type) {
            case 'banIP':
                for (var p in pools){
                    if (pools[p].stratumServer)
                        pools[p].stratumServer.addBannedIP(message.ip);
                }
                break;

            case 'reloadpool':
                if (message.coin) {
                    var messageCoin = message.coin.toLowerCase();
                    var poolTarget = Object.keys(pools).filter(function(p){
                        return p.toLowerCase() === messageCoin;
                    })[0];
                    poolConfigs  = JSON.parse(message.pools);
                    createAndStartPool(messageCoin);
                }
                break;

            case 'blocknotify':

                if (message.coin) {
                    var messageCoin = message.coin.toLowerCase();
                    var poolTarget = Object.keys(pools).filter(function(p){
                        return p.toLowerCase() === messageCoin;
                    })[0];

                    if (poolTarget)
                        pools[poolTarget].processBlockNotify(message.hash, 'blocknotify message');
                }
                break;
        }
    });


    var createAndStartPool = function(coin){
        var poolOptions = poolConfigs[coin];
        var myAuxes = poolConfigs[coin].auxes;

        var logSystem = 'Pool';
        var logComponent = coin;
        var logSubCat = 'Thread ' + (parseInt(forkId) + 1);

        var shareProcessor = new ShareProcessor(logger, poolOptions);

        var handlers = {
            auth: function(port, workerName, password, authCallback){
                // auth stub
                authCallback(true);
            },
            share: function(isValidShare, isValidBlock, data, coin, aux){
                // perform share processing
                shareProcessor.handleShare(isValidShare, isValidBlock, data, coin, aux);
            },
            auxblock: function(isValidBlock, height, hash, tx, diff, coin){
                // auxillary block processing
                shareProcessor.handleAuxBlock(isValidBlock, height, hash, tx, diff, coin);
            },
            diff: function(workerName, diff){
                // diff update stub
            }
        };

        var authorizeFN = function (ip, port, workerName, password, callback) {
            handlers.auth(port, workerName, password, function(authorized){
                var authString = authorized ? 'Authorized' : 'Unauthorized ';

                logger.debug(logSystem, logComponent, logSubCat, authString + ' ' + workerName + ':' + password + ' [' + ip + ']');
                callback({
                    error: null,
                    authorized: authorized,
                    disconnect: false
                });
            });
        };


        var pool = Stratum.createPool(poolOptions, authorizeFN, logger);
        pool.on('share', function(isValidShare, isValidBlock, data) {

            var shareData = JSON.stringify(data);

            if (data.blockHash && !isValidBlock) {
                logger.debug(logSystem, logComponent, logSubCat, 'We thought a block was found but it was rejected by the daemon, share data: ' + shareData);
            }
            else if (isValidBlock) {
                logger.info(logSystem, logComponent, logSubCat, 'Block found: ' + data.blockHash);
            }

            if (isValidShare) {
                logger.debug(logSystem, logComponent, logSubCat, 'Share accepted at diff ' + data.difficulty + '/' + data.shareDiff + ' by ' + data.worker + ' [' + data.ip + ']' );
            }
            else if (!isValidShare) {
                logger.fatal(logSystem, logComponent, logSubCat, 'Share rejected: ' + shareData);
            }

            handlers.share(isValidShare, isValidBlock, data, poolOptions.coin.name, false)

            //loop through auxcoins
            for(var i = 0; i < myAuxes.length; i++) {
                coin = myAuxes[i].name;
                handlers.share(isValidShare, false, data, coin, true);
            }
        }).on('auxblock', function(symbol, height, hash, tx, diff, mnr){
            for(var i = 0; i < myAuxes.length; i++) {
                if (myAuxes[i].symbol == symbol) {
                    coin = myAuxes[i].name;
                }
            }
            handlers.auxblock(true, height, hash, tx, diff, coin);
        }).on('difficultyUpdate', function(workerName, diff){
            logger.debug(logSystem, logComponent, logSubCat, 'Difficulty update to diff ' + diff + ' workerName=' + JSON.stringify(workerName));
            handlers.diff(workerName, diff);
        }).on('log', function(severity, text) {
            logger.debug(logSystem, logComponent, logSubCat, text);
        }).on('banIP', function(ip, worker){
            process.send({type: 'banIP', ip: ip});
        });

        pool.start();
        pools[poolOptions.coin.name] = pool;
    }

    Object.keys(poolConfigs).forEach(function(coin) {
        createAndStartPool(coin);
    });
};
