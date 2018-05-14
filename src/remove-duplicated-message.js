import Promise from 'bluebird';
import Redis from 'redis';
import program from 'commander';

Promise.promisifyAll(Redis.RedisClient.prototype);
Promise.promisifyAll(Redis.Multi.prototype);

const list = (val) => {
    return val.split(',').map((item) => {
        return item.trim();
    });
}

// parameters
program
    .version('1.0.0')
    .option('--redis-url [url]', 'Redis connect URL', 'redis://127.0.0.1:6379')
    .option('--redis-input-channel [input]', 'Redis subscribe channel', 'p_tploader')
    .option('--redis-output-channel [output]', 'Redis publish channel', 'clear_tploader')
    .option('--field-list [list]', 'Field list to be compared with', list, ['startTs', 'endTs'])
    .parse(process.argv);

// constants
const BUFFER_MAX_LENGTH = 500;

// global resources
const pubClient = Redis.createClient(program.redisUrl);
const subClient = Redis.createClient(program.redisUrl);
const buffer = [];

const messageHandler = async (channel, msg) => {
    try {
        const message = JSON.parse(msg);
        
        if (buffer.length === 0) {
            buffer.push(message);
            pubClient.publish(program.redisOutputChannel, msg);
    
            return;
        }
    
        const duplicated = buffer.reduce((prev, curr) => {
            if (prev === true) {
                return prev;
            }
    
            const same = program.fieldList.reduce((prevField, currField) => {
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
            pubClient.publish(program.redisOutputChannel, msg);
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

        pubClient.publish(program.redisOutputChannel, msg);
    }
}

subClient.subscribe(program.redisInputChannel);
subClient.on('message', messageHandler);
