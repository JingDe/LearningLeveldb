
void BlockHandle::EncodeTo(std::string* dst) const
{
	assert(offset_ != ~static_cast<uint64_t>(0));
	assert(size_ != ~static_cast<uint64_t>(0));
	PutVarint64(dst, offset_);
	PutVarint64(dst, size_);
}

void Footer::EncodeTo(std::string* dst) const
{
	const size_t original_size = dst->size();
	metaindex_handle_.EncodeTo(dst);
	index_handle_.EncodeTo(dst);
	dst->resize(2* BlockHandle::kMaxEncodedLength);
	PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
	PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
	assert(dst->size() == original_size + kEncodedLength);
	(void)original_size; // Disable unused variable warning.
}