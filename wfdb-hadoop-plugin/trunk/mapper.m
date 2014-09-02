function out=mapper(record,fs)

%Read load record data
load(record);
[M,N]=size(val);


#Calculate largest peak in each signal
df=50; %Peak search tolerance in Hz
dw=(fs/2)/N;  %Peak tolerance in bins

for m=1:M
	X=abs(fft(val(m,:)));
        [mv,ind]=max(X);
end
