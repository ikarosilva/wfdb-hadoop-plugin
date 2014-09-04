function p=mapper(record,fs)

%Read load record data
load(record);
[M,N]=size(val);

#Calculate largest peak in each signal
df=50; %Peak search tolerance in Hz
N2=round(N/2);
delta_f=fs/N;
dw=floor(df/delta_f);  %Peak tolerance window in bins

p=zeros(M,3);


for m=1:M

	%subtract dc component
	val(m,:)=val(m,:)-mean(val(m,:));
	
	%Get First FFT peak
	X=abs(fft(val(m,:)));
	X=X(1:N2);
        [mv,peak1]=max(X);
	mn_bin=max(peak1-dw,1);
	mx_bin=min(peak1+dw,N2);
        X(mn_bin:mx_bin)=-inf;
	
        %Get second peak, outside the threshold window
	[mv,peak2]=max(X);
	mn_bin=max(peak2-dw,1);
	mx_bin=min(peak2+dw,N2);
        X(mn_bin:mx_bin)=-inf;

	%Get third peak, outside the threshold window
	[mv,peak3]=max(X);

	%Conver peaks to frequency and add to buffer
	p(m,1)=(peak1-1)*delta_f;
	p(m,2)=(peak2-1)*delta_f;
	p(m,3)=(peak3-1)*delta_f;	
end

%Output to standard stream
disp(p(:))

