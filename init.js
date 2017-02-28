var fs = require('fs');
var path = require('path');
var os = require('os');
var cluster = require('cluster');

var async = require('async');
var extend = require('extend');

var PoolLogger = require('log4js');
var CliListener = require('./libs/cliListener.js');
var PoolWorker = require('./libs/poolWorker.js');

JSON.minify = JSON.minify || require("node-json-minify");

if (!fs.existsSync('config.json')){
    console.log('config.json file does not exist. Read the installation/setup instructions.');
    return;
}

var portalConfig = JSON.parse(JSON.minify(fs.readFileSync("config.json", {encoding: 'utf8'})));
var poolConfigs;

var logger = PoolLogger.getLogger();

//try {
//    logger.info('New Relic');
//    require('newrelic');
//    if (cluster.isMaster)
//        logger.debug('NewRelic', 'Monitor', 'New Relic initiated');
//} catch(e) {}

//Try to give process ability to handle 100k concurrent connections
try{
    var posix = require('posix');
    try {
    logger.info('Setting POSIX');
        posix.setrlimit('nofile', { soft: 100000, hard: 100000 });
    logger.info('POSIX Set');
    }
    catch(e){
    logger.info(e);
    logger.error('Must be ran as root');
        if (cluster.isMaster){
            logger.warn('POSIX', 'Connection Limit', '(Safe to ignore) Must be ran as root to increase resource limits');}
    }
    finally {
        // Find out which user used sudo through the environment variable
        var uid = parseInt(process.env.SUDO_UID);
        // Set our server's uid to that user
        if (uid) {
            process.setuid(uid);
        logger.info("UID Set");
            logger.debug('POSIX', 'Connection Limit', 'Raised to 100K concurrent connections, now running as non-root user: ' + process.getuid());
        logger.info('POSIX Msg');
        }
    }
}
catch(e){
    logger.info('POSIX Not Installed');
    if (cluster.isMaster)
        logger.debug('POSIX', 'Connection Limit', '(Safe to ignore) POSIX module not installed and resource (connection) limit was not raised');
}

logger.info('Run Workers');
if (cluster.isWorker){
    new PoolWorker(logger);
    return;
}

//Read all pool configs from pool_configs and join them with their coin profile
var buildPoolConfigs = function(){
    var configs = {};
    var configDir = 'pool_configs/';

    var poolConfigFiles = [];


    /* Get filenames of pool config json files that are enabled */
    fs.readdirSync(configDir).forEach(function(file){
        if (!fs.existsSync(configDir + file) || path.extname(configDir + file) !== '.json') return;
        var poolOptions = JSON.parse(JSON.minify(fs.readFileSync(configDir + file, {encoding: 'utf8'})));
        if (!poolOptions.enabled) return;
        poolOptions.fileName = file;
        poolConfigFiles.push(poolOptions);
    });


    /* Ensure no pool uses any of the same ports as another pool */
    for (var i = 0; i < poolConfigFiles.length; i++){
        var ports = Object.keys(poolConfigFiles[i].ports);
        for (var f = 0; f < poolConfigFiles.length; f++){
            if (f === i) continue;
            var portsF = Object.keys(poolConfigFiles[f].ports);
            for (var g = 0; g < portsF.length; g++){
                if (ports.indexOf(portsF[g]) !== -1){
                    logger.error('Master', poolConfigFiles[f].fileName, 'Has same configured port of ' + portsF[g] + ' as ' + poolConfigFiles[i].fileName);
                    process.exit(1);
                    return;
                }
            }

            if (poolConfigFiles[f].coin === poolConfigFiles[i].coin){
                logger.error('Master', poolConfigFiles[f].fileName, 'Pool has same configured coin file coins/' + poolConfigFiles[f].coin + ' as ' + poolConfigFiles[i].fileName + ' pool');
                process.exit(1);
                return;
            }

        }
    }

    poolConfigFiles.forEach(function(poolOptions){

        poolOptions.coinFileName = poolOptions.coin;
        for (var i=0; i < poolOptions.auxes.length; i++){
            var auxFilePath = 'coins/' + poolOptions.auxes[i].coin;
            if (!fs.existsSync(auxFilePath)) {
                logger.warn('Aux', poolOptions.auxes[i].coin, 'could not find file: ' + auxFilePath);
                return;
            }

            var auxProfile = JSON.parse(JSON.minify(fs.readFileSync(auxFilePath, {encoding: 'utf8'})));
            poolOptions.auxes[i].coin = auxProfile;
            poolOptions.auxes[i].coin.name = poolOptions.auxes[i].coin.name.toLowerCase();
        }

        var coinFilePath = 'coins/' + poolOptions.coinFileName;
        if (!fs.existsSync(coinFilePath)){
            logger.warn('Master', poolOptions.coinFileName, 'could not find file: ' + coinFilePath);
            return;
        }

        var coinProfile = JSON.parse(JSON.minify(fs.readFileSync(coinFilePath, {encoding: 'utf8'})));
        poolOptions.coin = coinProfile;
        poolOptions.coin.name = poolOptions.coin.name.toLowerCase();

        if (poolOptions.coin.name in configs) {
            logger.warn('Master', poolOptions.fileName, 'coins/' + poolOptions.coinFileName
                + ' has same configured coin name ' + poolOptions.coin.name + ' as coins/'
                + configs[poolOptions.coin.name].coinFileName + ' used by pool config '
                + configs[poolOptions.coin.name].fileName);

            process.exit(1);
            return;
        }

        for (var option in portalConfig.defaultPoolConfigs){
            if (!(option in poolOptions)){
                var toCloneOption = portalConfig.defaultPoolConfigs[option];
                var clonedOption = {};
                if (toCloneOption.constructor === Object)
                    extend(true, clonedOption, toCloneOption);
                else
                    clonedOption = toCloneOption;
                poolOptions[option] = clonedOption;
            }
        }

        configs[poolOptions.coin.name] = poolOptions;
    });
    return configs;
};

var buildAuxConfigs = function(){
    var configs = {};
    var configDir = 'aux_configs/';

    var poolConfigFiles = [];


    /* Get filenames of pool config json files that are enabled */
    fs.readdirSync(configDir).forEach(function(file){
        if (!fs.existsSync(configDir + file) || path.extname(configDir + file) !== '.json') return;
        var poolOptions = JSON.parse(JSON.minify(fs.readFileSync(configDir + file, {encoding: 'utf8'})));
        if (!poolOptions.enabled) return;
        poolOptions.fileName = file;
        poolConfigFiles.push(poolOptions);
    });

    poolConfigFiles.forEach(function(poolOptions){

        poolOptions.coinFileName = poolOptions.coin;

        var poolFilePath = 'coins/' + poolOptions.coinFileName;
        if (!fs.existsSync(poolFilePath)){
            logger.warn('Master', poolOptions.coinFileName, 'could not find file: ' + poolFilePath);
            return;
        }

        var poolProfile = JSON.parse(JSON.minify(fs.readFileSync(poolFilePath, {encoding: 'utf8'})));
        poolOptions.coin = poolProfile;
        poolOptions.coin.name = poolOptions.coin.name.toLowerCase();
        configs[poolOptions.coin.name] = poolOptions;

        for (var option in portalConfig.defaultPoolConfigs){
            if (!(option in poolOptions)){
                var toCloneOption = portalConfig.defaultPoolConfigs[option];
                var clonedOption = {};
                if (toCloneOption.constructor === Object)
                    extend(true, clonedOption, toCloneOption);
                else
                    clonedOption = toCloneOption;
                poolOptions[option] = clonedOption;
            }
        }
    });
    return configs;
};

var spawnPoolWorkers = function(){

    Object.keys(poolConfigs).forEach(function(coin){
        var p = poolConfigs[coin];

        if (!Array.isArray(p.daemons) || p.daemons.length < 1){
            logger.error('Master', coin, 'No daemons configured so a pool cannot be started for this coin.');
            delete poolConfigs[coin];
        }
    });

    if (Object.keys(poolConfigs).length === 0){
        logger.warn('Master', 'PoolSpawner', 'No pool configs exists or are enabled in pool_configs folder. No pools spawned.');
        return;
    }


    var serializedConfigs = JSON.stringify(poolConfigs);

    var numForks = (function(){
        if (!portalConfig.clustering || !portalConfig.clustering.enabled)
            return 1;
        if (portalConfig.clustering.forks === 'auto')
            return os.cpus().length;
        if (!portalConfig.clustering.forks || isNaN(portalConfig.clustering.forks))
            return 1;
        return portalConfig.clustering.forks;
    })();

    var poolWorkers = {};

    var createPoolWorker = function(forkId){
        var worker = cluster.fork({
            workerType: 'pool',
            forkId: forkId,
            pools: serializedConfigs,
            portalConfig: JSON.stringify(portalConfig)
        });
        worker.forkId = forkId;
        worker.type = 'pool';
        poolWorkers[forkId] = worker;
        worker.on('exit', function(code, signal){
            logger.error('Master', 'PoolSpawner', 'Fork ' + forkId + ' died, spawning replacement worker...');
            setTimeout(function(){
                createPoolWorker(forkId);
            }, 2000);
        }).on('message', function(msg){
            switch(msg.type){
                case 'banIP':
                    Object.keys(cluster.workers).forEach(function(id) {
                        if (cluster.workers[id].type === 'pool'){
                            cluster.workers[id].send({type: 'banIP', ip: msg.ip});
                        }
                    });
                    break;
            }
        });
    };

    var i = 0;
    var spawnInterval = setInterval(function(){
        createPoolWorker(i);
        i++;
        if (i === numForks){
            clearInterval(spawnInterval);
            logger.debug('Master', 'PoolSpawner', 'Spawned ' + Object.keys(poolConfigs).length + ' pool(s) on ' + numForks + ' thread(s)');
        }
    }, 250);

};


var startCliListener = function(){

    var cliPort = portalConfig.cliPort;

    var listener = new CliListener(cliPort);
    listener.on('log', function(text){
        logger.debug('Master', 'CLI', text);
    }).on('command', function(command, params, options, reply){

        switch(command){
            case 'blocknotify':
                Object.keys(cluster.workers).forEach(function(id) {
                    cluster.workers[id].send({type: 'blocknotify', coin: params[0], hash: params[1]});
                });
                reply('Pool workers notified');
                break;
            case 'reloadpool':
                poolConfigs = buildPoolConfigs();
                Object.keys(cluster.workers).forEach(function(id) {
                    cluster.workers[id].send({type: 'reloadpool', coin: params[0], pools: JSON.stringify(poolConfigs) });
                });
                reply('reloaded pool ' + params[0]);
                break;
            default:
                reply('unrecognized command "' + command + '"');
                break;
        }
    }).start();
};

(function init(){

    poolConfigs = buildPoolConfigs();

    auxConfigs = buildAuxConfigs();

    spawnPoolWorkers();

    setTimeout(function(){
        startCliListener();
    }, 10000);
})();
