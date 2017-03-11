import should from 'should'
import { ConnectionConfig, ReaderConfig } from '../src/config'

describe('ConnectionConfig', () => {
  let config = null
  beforeEach(() => config = new ConnectionConfig())

  it('should use all defaults if nothing is provided', () => config.maxInFlight.should.eql(1))

  it('should validate with defaults', () => {
    const check = () => config.validate()
    return check.should.not.throw()
  })

  it('should remove an unrecognized option', () => {
    config = new ConnectionConfig({ unknownOption: 20 })
    return config.should.not.have.property('unknownOption')
  })

  describe('isNonEmptyString', () => {
    it('should correctly validate a non-empty string', () => {
      const check = () => config.isNonEmptyString('name', 'worker')
      return check.should.not.throw()
    })

    it('should throw on an empty string', () => {
      const check = () => config.isNonEmptyString('name', '')
      return check.should.throw()
    })

    return it('should throw on a non-string', () => {
      const check = () => config.isNonEmptyString('name', {})
      return check.should.throw()
    })
  })

  describe('isNumber', () => {
    it('should validate with a value equal to the lower bound', () => {
      const check = () => config.isNumber('maxInFlight', 1, 1)
      return check.should.not.throw()
    })
    it('should validate with a value between the lower and upper bound', () => {
      const check = () => config.isNumber('maxInFlight', 5, 1, 10)
      return check.should.not.throw()
    })
    it('should validate with a value equal to the upper bound', () => {
      const check = () => config.isNumber('maxInFlight', 10, 1, 10)
      return check.should.not.throw()
    })
    it('should not validate with a value less than the lower bound', () => {
      const check = () => config.isNumber('maxInFlight', -1, 1)
      return check.should.throw()
    })
    it('should not validate with a value greater than the upper bound', () => {
      const check = () => config.isNumber('maxInFlight', 11, 1, 10)
      return check.should.throw()
    })
    return it('should not validate against a non-number', () => {
      const check = () => config.isNumber('maxInFlight', null, 0)
      return check.should.throw()
    })
  })

  describe('isNumberExclusive', () => {
    it('should not validate with a value equal to the lower bound', () => {
      const check = () => config.isNumberExclusive('maxInFlight', 1, 1)
      return check.should.throw()
    })
    it('should validate with a value between the lower and upper bound', () => {
      const check = () => config.isNumberExclusive('maxInFlight', 5, 1, 10)
      return check.should.not.throw()
    })
    it('should not validate with a value equal to the upper bound', () => {
      const check = () => config.isNumberExclusive('maxInFlight', 10, 1, 10)
      return check.should.throw()
    })
    it('should not validate with a value less than the lower bound', () => {
      const check = () => config.isNumberExclusive('maxInFlight', -1, 1)
      return check.should.throw()
    })
    it('should not validate with a value greater than the upper bound', () => {
      const check = () => config.isNumberExclusive('maxInFlight', 11, 1, 10)
      return check.should.throw()
    })
    return it('should not validate against a non-number', () => {
      const check = () => config.isNumberExclusive('maxInFlight', null, 0)
      return check.should.throw()
    })
  })

  describe('isBoolean', () => {
    it('should validate against true', () => {
      const check = () => config.isBoolean('tls', true)
      return check.should.not.throw()
    })
    it('should validate against false', () => {
      const check = () => config.isBoolean('tls', false)
      return check.should.not.throw()
    })
    it('should not validate against null', () => {
      const check = () => config.isBoolean('tls', null)
      return check.should.throw()
    })
    return it('should not validate against a non-boolean value', () => {
      const check = () => config.isBoolean('tls', 'hi')
      return check.should.throw()
    })
  })

  describe('isBareAddresses', () => {
    it('should validate against a validate address list of 1', () => {
      const check = () => config.isBareAddresses('nsqdTCPAddresses', ['127.0.0.1:4150'])
      return check.should.not.throw()
    })
    it('should validate against a validate address list of 2', () => {
      const check = function () {
        const addrs = ['127.0.0.1:4150', 'localhost:4150']
        return config.isBareAddresses('nsqdTCPAddresses', addrs)
      }
      return check.should.not.throw()
    })
    return it('should not validate non-numeric port', () => {
      const check = () => config.isBareAddresses('nsqdTCPAddresses', ['localhost'])
      return check.should.throw()
    })
  })

  return describe('isLookupdHTTPAddresses', () => {
    it('should validate against a validate address list of 1', () => {
      const check = () => config.isLookupdHTTPAddresses('lookupdHTTPAddresses', ['127.0.0.1:4150'])
      return check.should.not.throw()
    })
    it('should validate against a validate address list of 2', () => {
      const check = function () {
        const addrs = [
          '127.0.0.1:4150',
          'localhost:4150',
          'http://localhost/nsq/lookup',
          'https://localhost/nsq/lookup'
        ]
        return config.isLookupdHTTPAddresses('lookupdHTTPAddresses', addrs)
      }
      return check.should.not.throw()
    })
    it('should not validate non-numeric port', () => {
      const check = () => config.isLookupdHTTPAddresses('lookupdHTTPAddresses', ['localhost'])
      return check.should.throw()
    })
    return it('should not validate non-HTTP/HTTPs address', () => {
      const check = () => config.isLookupdHTTPAddresses('lookupdHTTPAddresses', ['localhost'])
      return check.should.throw()
    })
  })
})

describe('ReaderConfig', () => {
  let config = null
  beforeEach(() => config = new ReaderConfig())

  it('should use all defaults if nothing is provided', () => config.maxInFlight.should.eql(1))

  it('should validate with defaults', () => {
    const check = function () {
      config = new ReaderConfig({ nsqdTCPAddresses: ['127.0.0.1:4150'] })
      return config.validate()
    }

    return check.should.not.throw()
  })

  return it('should convert a string address to an array', () => {
    config = new ReaderConfig({ lookupdHTTPAddresses: '127.0.0.1:4161' })
    config.validate()

    return config.lookupdHTTPAddresses.length.should.equal(1)
  })
})
