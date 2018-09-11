# update-stream

Creates a transform stream that updates/appends records in a stream from a Map based on a version and key field.

```js
const update = require('update-stream');

myObjectStream.pipe(update({
  keyField: 'id',
  versionField: '_v',
  changes: new Map()
}));
```

Changes that have a newer version than an exisitng record will be kept; changes that have an older version than an exising record will be skipped, and changes that had no corresponding existing record will be appended to the stream.
