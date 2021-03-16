module.exports = {
  env: {
    node: true,
  },
  extends: ['eslint:recommended'],
  parserOptions: {
    ecmaVersion: 11,
    sourceType: 'module',
  },
  plugins: [],
  rules: {},
  settings: {},
  globals: {
    after: false,
    afterEach: false,
    before: false,
    browser: false,
    describe: false,
    beforeEach: false,
    it: false,
  },
}
