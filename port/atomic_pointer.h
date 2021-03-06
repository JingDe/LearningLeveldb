#ifndef ATOMIC_POINTER_H
#define ATOMIC_POINTER_H


namespace LearningLeveldb{

inline void MemoryBarrier() {
	
  // See http://gcc.gnu.org/ml/gcc/2003-04/msg01180.html for a discussion on
  // this idiom. Also see http://en.wikipedia.org/wiki/Memory_ordering.
  __asm__ __volatile__("" : : : "memory");
}



class AtomicPointer {
 private:
  void* rep_;
 public:
  AtomicPointer() { }
  explicit AtomicPointer(void* p) : rep_(p) 
  {}
  inline void* NoBarrier_Load() const { return rep_; }
  inline void NoBarrier_Store(void* v) { rep_ = v; }
  inline void* Acquire_Load() const {
    void* result = rep_;
    __asm__ __volatile__("" : : : "memory");
    return result;
  }
  inline void Release_Store(void* v) {
    __asm__ __volatile__("" : : : "memory");
    rep_ = v;
  }
};

}

#endif