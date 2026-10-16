from .home_routes import home_bp
from .auth_routes import auth_bp
from .limpeza_historico_routes import limpeza_bp

def register_blueprints(app):
    app.register_blueprint(auth_bp)
    app.register_blueprint(home_bp)
    app.register_blueprint(limpeza_bp)
