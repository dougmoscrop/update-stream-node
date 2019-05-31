'use strict';

const assert = require('assert');

const { Transform } = require('stream');

module.exports = (options = {}) => {
    const {
        keyField,
        versionField = 'version',
        changes,
        batches = false,
    } = options;

    assert(!!keyField, 'must provide keyField');
    assert(changes instanceof Map, 'changes must be a Map');

    return new Transform({
            objectMode: true,
            transform(chunk, enc, callback) {
                const records = Array.isArray(chunk) ? chunk : [chunk];

                for (let r = 0, length = records.length; r < length; r += 1) {
                    const row = records[r];
                    const key = row[keyField];
                    const change = changes.get(key);

                    if (change) {
                        changes.delete(key);

                        if (change[versionField] > row[versionField]) {
                            records[r] = change;
                        }
                    }

                    if (batches) {
                        continue;
                    }

                    this.push(records[r]);
                }

                if (batches) {
                    callback(null, records);
                } else {
                    callback();
                }
            },
            flush(callback) {
                // any changes that were not removed during the transform
                // are new, and need to be appended
                if (batches) {
                    if (changes.size) {
                        const values = Array.from(changes.values());
                        this.push(values);
                    }
                } else {
                    changes.forEach(change => {
                        this.push(change);
                    });
                }
                callback();
            },
        });
};
