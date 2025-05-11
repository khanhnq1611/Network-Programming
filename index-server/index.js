const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const { getChecksum } = require('../common/hashing');
const types = require('../common/messageTypes');

const PORT = 8000;
const REPLICATION_FACTOR = 3;

const wss = new WebSocket.Server({ port: PORT });
const nodes = new Map();       // nodeId => ws
const files = new Map();       // fileId => { name, size, date, checksum, nodes: [nodeId] }

wss.on('connection', ws => {
  ws.on('message', async msg => {
    let m = JSON.parse(msg);
    switch (m.type) {
      case types.REGISTER_NODE:
        nodes.set(m.nodeId, ws);
        console.log(`Node registered: ${m.nodeId}`);
        break;

      case types.UPLOAD: {
        // assign file ID + choose replication nodes
        let fileId = uuidv4().slice(0, 8).toUpperCase();
        let targetNodeIds = Array.from(nodes.keys()).slice(0, REPLICATION_FACTOR);
        files.set(fileId, {
          name: m.name, size: m.size, date: m.date,
          checksum: m.checksum, nodes: targetNodeIds
        });

        // replicate and wait for STORE_ACK from each node
        let acks = 0;
        targetNodeIds.forEach(nodeId => {
          const nodeWs = nodes.get(nodeId);
          nodeWs.send(JSON.stringify({
            type: types.STORE,
            fileId, name: m.name,
            data: m.data
          }));
          nodeWs.once('message', ackMsg => {
            const a = JSON.parse(ackMsg);
            if (a.type === types.STORE_ACK && a.fileId === fileId) {
              acks++;
              if (acks === targetNodeIds.length) {
                ws.send(JSON.stringify({
                  type: types.UPLOAD_ACK,
                  fileId, checksum: m.checksum
                }));
              }
            }
          });
        });
        break;
      }

      case types.LIST:
        // reply with all file metadata
        ws.send(JSON.stringify({
          type: types.LIST_RESPONSE,
          files: Array.from(files.entries()).map(([id, meta]) => ({
            id, ...meta
          }))
        }));
        break;

      case types.DOWNLOAD: {
        let info = files.get(m.fileId);
        if (!info) {
          ws.send(JSON.stringify({ type: types.RETRIEVE_ACK, error: 'Not found' }));
          break;
        }
        // ask first replica (now pass the original filename)
        let nodeWs = nodes.get(info.nodes[0]);
        nodeWs.send(JSON.stringify({
          type: types.RETRIEVE,
          fileId: m.fileId,
          name: info.name
        }));
        const sendBack = dataMsg => {
          let d = JSON.parse(dataMsg);
          if (d.type === types.RETRIEVE_ACK && d.fileId === m.fileId) {
            ws.send(JSON.stringify(d));
            nodeWs.removeListener('message', sendBack);
          }
        };
        nodeWs.on('message', sendBack);
        break;
      }

      case types.DELETE: {
        let info = files.get(m.fileId);
        if (!info) {
          ws.send(JSON.stringify({ type: types.DELETE_ACK, error: 'Not found' }));
          break;
        }
        info.nodes.forEach(nodeId => {
          let nodeWs = nodes.get(nodeId);
          nodeWs.send(JSON.stringify({ type: types.DELETE, fileId: m.fileId }));
        });
        files.delete(m.fileId);
        ws.send(JSON.stringify({ type: types.DELETE_ACK, fileId: m.fileId }));
        break;
      }
    }
  });
});

console.log(`Index server listening on ws://localhost:${PORT}`);
