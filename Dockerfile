FROM node:8.4

# Version 0.3.8
# RUN wget --quiet "https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.3.8.linux-amd64.go1.6.2.tar.gz"
# RUN tar zvxf "nsq-0.3.8.linux-amd64.go1.6.2.tar.gz"
# ENV PATH="${PATH}:/nsq-0.3.8.linux-amd64.go1.6.2/bin"

# Version 1.0.0-compat
RUN wget --quiet "https://github.com/nsqio/nsq/releases/download/v1.0.0-compat/nsq-1.0.0-compat.linux-amd64.go1.8.tar.gz"
RUN tar zvxf "nsq-1.0.0-compat.linux-amd64.go1.8.tar.gz"
ENV PATH="${PATH}:/nsq-1.0.0-compat.linux-amd64.go1.8/bin"

WORKDIR /nsqjs
ADD package.json /nsqjs/package.json
RUN npm install --silent

ADD . /nsqjs

CMD ["npm", "test"]
