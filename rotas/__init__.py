from .home_routes import home_bp
from .auth_routes import auth_bp
from .limpeza_historico_routes import limpeza_bp
from .plan51_routes import plan51_bp
from .api_dados_routes import api_dados_bp
from .notas_see_routes import notas_see_bp

def register_blueprints(app):
    app.register_blueprint(auth_bp)
    app.register_blueprint(home_bp)
    app.register_blueprint(limpeza_bp)
    app.register_blueprint(plan51_bp)
    app.register_blueprint(api_dados_bp)
    app.register_blueprint(notas_see_bp)
