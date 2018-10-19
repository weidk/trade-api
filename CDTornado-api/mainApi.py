import tornado.web
import tornado.websocket
import tornado.httpserver
import tornado.ioloop


class WebSocketHandler(tornado.websocket.WebSocketHandler):
    def check_origin(self, origin):
        return True

    def open(self):
        self.write_message("Connection is open!")

    def on_message(self, message):
        print(message)
        self.write_message(u"Your message was: " + message)

    def on_close(self):
        print("Connection is closed!")


if __name__ == '__main__':
    ws_app = tornado.web.Application([
        (r'/wsapi', WebSocketHandler)
    ])
    server = tornado.httpserver.HTTPServer(ws_app)
    server.listen(8080)
    tornado.ioloop.IOLoop.instance().start()