function Y=shuffle(x,M)
%
% Generates L surrogate time series from x
% by with AmplitudeAdjustedPhaseShuffle
%Underlying assumption is that the Null Hypothesis consists
%of linear dynamics with possibly non-linear, monotonically increasing,
%measurement function.
%
%  1. Amp transform original data to Gaussian distribution
%  2. Phase randomize #1
%  3. Amp transform #2 to original
% Auto-correlation function should be similar but not exact!

x=x(:);
N=length(x(:));
Y=zeros(N,M);

for m=1:M
    %Step 1
    y=randn(N,1);
    y=amplitudeTransform(x,y,N);
    
    %Step 2
    y=phaseShuffle(y,N);
    
    %Step 3
    Y(:,m)=amplitudeTransform(y,x,N);
end

end

function target=amplitudeTransform(x,target,N)

%Steps:
%1. Sort the source by increasing amp
%2. Sort target as #1
%3. Swap source amp by target amp
%4. Sort #3 by increasing time index of #1
X=[[1:N]' x];
X=sortrows(X,2);
target=[X(:,1) sort(target)];
target=sortrows(target,1);
target=target(:,2);
end


function y=phaseShuffle(x,N)

%%Shuffle spectrum
N=11;
x=randn(N,1);
X=fft(x);
Y=X;
mid=floor(N/2)+ mod(N,2);
phi=2*pi*rand(mid-1,1); %Generate random phase
Y(2:mid)=abs(X(2:mid)).*cos(phi) + j*abs(X(2:mid)).*sin(phi);
if(~mod(N,2))
    %Even series has Nyquist in the middle+1 because of DC
    Y(mid+2:end)=conj(flipud(Y(2:mid)));
    Y(mid+1)=X(mid+1);
else
    %Odd series is fully symetric except for DC
    Y(mid+1:end)=conj(flipud(Y(2:mid)));
end

y=real(ifft(Y));
end