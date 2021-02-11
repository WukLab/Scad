/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { initializeActionHandler, NodeActionRunner } = require('../runner');
const { LibdAction } = require("./libd")

function NodeActionService(config) {

    const Status = {
        ready: 'ready',
        starting: 'starting',
        running: 'running',
        stopped: 'stopped',
    };

    const ignoreRunStatus = config.allowConcurrent === undefined ? false : config.allowConcurrent.toLowerCase() === 'true';

    let status = Status.ready;
    let server = undefined;
    let userCodeRunner = undefined;

    function setStatus(newStatus) {
        if (status !== Status.stopped) {
            status = newStatus;
        }
    }

    /**
     * An ad-hoc format for the endpoints returning a Promise representing,
     * eventually, an HTTP response.
     *
     * The promised values (whether successful or not) have the form:
     * { code: int, response: object }
     *
     */
    function responseMessage(code, response) {
        return { code: code, response: response };
    }

    function errorMessage(code, errorMsg) {
        return responseMessage(code, { error: errorMsg });
    }

    /**
     * Indicates if we have been initialized which is determined by if we have
     * created a NodeActionRunner.
     * @returns {boolean}
     */
    this.initialized = function isInitialized(){
        return (typeof userCodeRunner !== 'undefined');
    };

    /**
     * Starts the server.
     *
     * @param app express app
     */
    this.start = function start(app) {
        server = app.listen(config.port, function() {
            var host = server.address().address;
            var port = server.address().port;
        });

        // This is required as http server will auto disconnect in 2 minutes, this to not auto disconnect at all
        server.timeout = 0;
    };

    // New APIs
    let actions = {}

    /** Returns a promise of a response to the /init invocation.
     *
     *  req.body = { main: String, code: String, binary: Boolean }
     */
    this.initCode = function initCode(req) {
        if (status === Status.ready && userCodeRunner === undefined) {
            setStatus(Status.starting);

            let body = req.body || {};
            let message = body.value || {};

            if (message.main && message.code && typeof message.main === 'string' && typeof message.code === 'string') {
                return doInit(message).then(_ => {
                    setStatus(Status.ready);
                    return responseMessage(200, { OK: true });
                }).catch(error => {
                    setStatus(Status.stopped);
                    let errStr = `Initialization has failed due to: ${error.stack ? String(error.stack) : error}`;
                    return Promise.reject(errorMessage(502, errStr));
                });
            } else {
                setStatus(Status.ready);
                let msg = 'Missing main/no code to execute.';
                return Promise.reject(errorMessage(403, msg));
            }
        } else if (userCodeRunner !== undefined) {
            let msg = 'Cannot initialize the action more than once.';
            console.error('Internal system error:', msg);
            return Promise.reject(errorMessage(403, msg));
        } else {
            let msg = `System not ready, status is ${status}.`;
            console.error('Internal system error:', msg);
            return Promise.reject(errorMessage(403, msg));
        }
    };

    /**
     * Returns a promise of a response to the /exec invocation.
     * Note that the promise is failed if and only if there was an unhandled error
     * (the user code threw an exception, or our proxy had an internal error).
     * Actions returning { error: ... } are modeled as a Promise successful resolution.
     *
     * req.body = { value: Object, meta { activationId : int } }
     */
    this.runCode = function runCode(req) {
        if (status === Status.ready && userCodeRunner !== undefined) {
            if (!ignoreRunStatus) {
                setStatus(Status.running);
            }

            // these are defensive checks against the expected interface invariants
            let msg = req && req.body || {};
            if (msg.value === null || msg.value === undefined) {
                msg.value = {};
            } else if (typeof msg.value !== 'object') {
                let errStr = `Internal system error: the argument must be a dictionary but has type '${typeof msg.value}'.`;
                console.error('Internal system error:', errStr);
                return Promise.reject(errorMessage(403, errStr));
            }

            if (errMsg = checkParameter(msg, 'activation_id')) { return Promise.reject(errorMessage(403, msg)) }

            let serverIp = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
            serverIp = serverIp.toString().replace('::ffff:', '');
            let serverUrl = `http://${serverIp}:${process.env['__OW_INVOKER_API_PORT']}`
            
            doAddAction(msg, msg.activation_id, serverUrl)

            console.log(`[action][${msg.activation_id}] Action created, prepare to run`)

            return doRun(msg, actions[msg.activation_id]).then(result => {
                if (!ignoreRunStatus) {
                    setStatus(Status.ready);
                }
                if (typeof result !== 'object') {
                    return errorMessage(502, 'The action did not return a dictionary.');
                } else {
                    return responseMessage(200, result);
                }
            }).catch(error => {
                let msg = `An error has occurred: ${error}`;
                setStatus(Status.stopped);
                return Promise.reject(errorMessage(502, msg));
            });
        } else {
            let msg = userCodeRunner ? `System not ready, status is ${status}.` : 'System not initialized.';
            console.error('Internal system error:', msg);
            return Promise.reject(errorMessage(403, msg));
        }
    };

    this.addAction = function addAction(req) {
        let msg = req && req.body || {};
        
        let aid = req.aid

        let serverIp = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
        serverIp = serverIp.toString().replace('::ffff:', '');
        let serverUrl = `http://${serverIp}:${process.env['__OW_INVOKER_API_PORT']}`

        doAddAction(msg, aid, serverUrl)

        return responseMessage(200, {});
    }

    this.addTransport = function addTransport(req) {
        let msg = req && req.body || {};

        let aid = req.aid

        checkParameter(msg, 'durl')
        let ret = actions[aid].init_transport(msg.durl)

        return responseMessage(200, {return: ret});
    }

    this.configTransport = function configTransport(req) {
        let msg = req && req.body || {};

        let aid = req.aid
        let tname = req.tname

        checkParameter(msg, 'durl')
        actions[aid].configTransport(tname, msg.configTransport)

        return responseMessage(200, {return: ret});
    }

    function doInit(message) {
        if (message.env && typeof message.env == 'object') {
            Object.keys(message.env).forEach(k => {
                let val = message.env[k];
                if (typeof val !== 'object' || val == null) {
                    process.env[k] = val ? val.toString() : "";
                } else {
                    process.env[k] = JSON.stringify(val);
                }
            });
        }

        return initializeActionHandler(message)
            .then(handler => {
                userCodeRunner = new NodeActionRunner(handler);
            })
            // emit error to activation log then flush the logs as this is the end of the activation
            .catch(error => {
                console.error('Error during initialization:', error);
                writeMarkers();
                return Promise.reject(error);
            });
    }

    function doAddAction(msg, aid, serverUrl) {
        if (aid in actions)
            return;

        console.log(`Try to init action ${aid} with serverUrl ${serverUrl}`)
        let action = new LibdAction(aid, serverUrl)
        if (msg.transports == null || msg.transports == undefined) {
            msg.transports = []
        } else if (!Array.isArray(msg.transports)) {
            let errStr = `Internal system error: require transports to be array of durls`;
            console.error('Internal system error:', errStr);
            return Promise.reject(errorMessage(403, errStr));
        }
        console.log(`Try to add transports for action ${aid}, transports: ${msg.transports.length}`)

        for (let i = 0; i < msg.transports.length; i++) {
            let ret = action.init_transport(msg.transports[i])
            console.log(`Return Value of Transport ${msg.transports[i]}: ${ret}`)
        }

        actions[aid] = action

        console.log(`[$aid] Finish config actions`)
    }

    function doRun(msg, action) {
        // Move per-activation keys to process env. vars with __OW_ (reserved) prefix
        Object.keys(msg).forEach(k => {
            if (typeof msg[k] === 'string' && k !== 'value') {
                let envVariable = '__OW_' + k.toUpperCase();
                process.env[envVariable] = msg[k];
            }
        });

        return userCodeRunner
            .run(msg.value, action)
            .then(result => {
                if (typeof result !== 'object') {
                    console.error(`Result must be of type object but has type "${typeof result}":`, result);
                }
                writeMarkers();
                return result;
            }).catch(error => {
                console.error(error);
                writeMarkers();
                return Promise.reject(error);
            });
    }

    function checkParameter(msg, field, d = undefined, f = undefined) {
        if (msg[field] == null || msg[field] == undefined) {
            if (d === undefined) {
                let errStr = `Internal system error: parameter check field for field "${field}"`;
                console.error('Internal system error:', errStr);
                return errStr
            } else {
                msg[field] = d
                return null
            }
        } else if (f != undefined && !f(msg[field])) {
            let errStr = `Internal system error: parameter check field for field "${field}"`;
            console.error('Internal system error:', errStr);
            return errStr
        }
    }

    function writeMarkers() {
        console.log('XXX_THE_END_OF_A_WHISK_ACTIVATION_XXX');
        console.error('XXX_THE_END_OF_A_WHISK_ACTIVATION_XXX');
    }
}

NodeActionService.getService = config => new NodeActionService(config);

module.exports = NodeActionService;
