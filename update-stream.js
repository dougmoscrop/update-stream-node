const assert = require('assert');

const { Transform } = require('stream');

module.exports = (options = {}) => {
    const {
        keyField,
        versionField,
        changes,
    } = options;

    assert(!!keyField, 'must provide keyField');
    assert(!!versionField, 'must provide versionField');
    assert(changes instanceof Map, 'changes must be a Map');

    return new Transform({
            objectMode: true,
            transform(row, enc, callback) {
                const key = row[keyField];
                const change = changes.get(key);

                if (change) {
                    changes.delete(key);

                    if (change[versionField] > row[versionField]) {
                        callback(null, change);
                        return;
                    }
                }

                callback(null, row);
            },
            flush(callback) {
                // any changes that were not removed during the transform
                // are new, and need to be appended
                Array.from(changes.values()).forEach(change => {
                    this.push(change);
                });
                callback();
            },
        });
};
