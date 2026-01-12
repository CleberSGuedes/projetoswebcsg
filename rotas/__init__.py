from .home_routes import home_bp
from .auth_routes import auth_bp

def register_blueprints(app):
    app.register_blueprint(auth_bp)
    app.register_blueprint(home_bp)
