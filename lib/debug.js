try {
  const debug = require('debug')
  module.exports = debug
} catch (e) {
  module.exports = () => {
    return () => {}
  }
}
