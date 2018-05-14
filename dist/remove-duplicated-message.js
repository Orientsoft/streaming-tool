'use strict';

var _bluebird = require('bluebird');

var _bluebird2 = _interopRequireDefault(_bluebird);

var _redis = require('redis');

var _redis2 = _interopRequireDefault(_redis);

var _commander = require('commander');

var _commander2 = _interopRequireDefault(_commander);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new _bluebird2.default(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return _bluebird2.default.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; }

_bluebird2.default.promisifyAll(_redis2.default.RedisClient.prototype);
_bluebird2.default.promisifyAll(_redis2.default.Multi.prototype);

const list = val => {
    return val.split(',').map(item => {
        return item.trim();
    });
};

// parameters
_commander2.default.version('1.0.0').option('--redis-url [url]', 'Redis connect URL', 'redis://127.0.0.1:6379').option('--redis-input-channel [input]', 'Redis subscribe channel', 'p_tploader').option('--redis-output-channel [output]', 'Redis publish channel', 'clear_tploader').option('--field-list [list]', 'Field list to be compared with', list, ['startTs', 'endTs']).parse(process.argv);

// constants
const BUFFER_MAX_LENGTH = 500;

// global resources
const pubClient = _redis2.default.createClient(_commander2.default.redisUrl);
const subClient = _redis2.default.createClient(_commander2.default.redisUrl);
const buffer = [];

const messageHandler = (() => {
    var _ref = _asyncToGenerator(function* (channel, msg) {
        try {
            const message = JSON.parse(msg);

            if (buffer.length === 0) {
                buffer.push(message);
                pubClient.publish(_commander2.default.redisOutputChannel, msg);

                return;
            }

            const duplicated = buffer.reduce(function (prev, curr) {
                if (prev === true) {
                    return prev;
                }

                const same = _commander2.default.fieldList.reduce(function (prevField, currField) {
                    if (prevField === false) {
                        return prevField;
                    }

                    if (curr[currField] === message[currField]) {
                        return true;
                    }

                    return false;
                }, true);

                return same;
            }, false);

            if (duplicated === false) {
                pubClient.publish(_commander2.default.redisOutputChannel, msg);
            } else {
                const now = new Date();
                console.log(`${now.toISOString()} [Dup] - (${channel}) ${JSON.stringify(message)}`);
            }

            buffer.push(message);
            if (buffer.length > BUFFER_MAX_LENGTH) {
                buffer.pop();
            }
        } catch (err) {
            // output log
            const now = new Date();
            console.log(`${now.toISOString()} [Err] - ${err.stack}`);

            pubClient.publish(_commander2.default.redisOutputChannel, msg);
        }
    });

    return function messageHandler(_x, _x2) {
        return _ref.apply(this, arguments);
    };
})();

subClient.subscribe(_commander2.default.redisInputChannel);
subClient.on('message', messageHandler);