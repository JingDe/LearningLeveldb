
void BlockHandle::EncodeTo(std::string* dst) const
{
	assert(offset_ != ~static_cast<uint64_t>(0));
	assert(size_ != ~static_cast<uint64_t>(0));
	PutVarint64(dst, offset_);
	PutVarint64(dst, size_);
}