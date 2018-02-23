
class MemEnvTest {
public:
	Env* env_;

	MemEnvTest(): env_(NewMemEnv(Env::Default())) {
	}
	~MemEnvTest() {
		delete env_;
	}
};