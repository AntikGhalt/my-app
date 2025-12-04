from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({"message": "ciao Paolo, funziona!", "path": "/"})

@app.route("/test")
def test():
    return jsonify({"message": "test endpoint ok", "path": "/test"})

@app.route("/run")
def run():
    return jsonify({"message": "run endpoint ok (dummy)", "path": "/run"})


@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "status": "error",
        "message": "route not found",
        "path": str(getattr(error, "description", "")),
        "available_routes": ["/", "/test", "/run"]
    }), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
