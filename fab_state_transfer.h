#ifndef fab_state_transfer_h
#define fab_state_transfer_h

class fab_state_transfer {
 public:
  virtual std::string marshal_state() = 0;
  virtual void unmarshal_state(std::string) = 0;
  virtual ~fab_state_transfer() {};
};

#endif
