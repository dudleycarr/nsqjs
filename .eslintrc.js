module.exports = {
  env: {
    es2020: true,
    node: true,
  },
  extends: ['eslint:recommended'],
  parserOptions: {
    ecmaVersion: 10,
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
