clear all;clc;close all

r='134-0';
!rec=134; sig=0;wqrs -r mghdb/mgh${rec} -s ${sig};ann2rr -r mghdb/mgh${rec} -a wqrs > rr-${rec}-${sig};cat rr-${rec}-${sig} | mse -n 20 | sed 's/^m.*//'> rr2mse-${rec}-${sig}
x=dlmread(['rr-' r]);
plot(x)
s=x(200:400);
y=shuffle2(s);

subplot(211)
plot(s,'o-','LineWidth',1)
grid on;
title('Orginal Series')

subplot(212)
plot(y,'x-r','LineWidth',1)
grid on;
title('Amplitude Shuffle Series')

figure
mse1=dlmread(['rr2mse-' r]);
plot(mse1(:,1),mse1(:,2),'b-o')
grid on;hold on


%Plot the average MSE curve from 20 simulations
% use to generate surrogate: !rm surr_* ; shuffle(['rr2mse-' r])
%for i in `ls surr_*` ;do cat ${i} | mse -n 20 | sed 's/^m.*//' >${i}-mse-out; done
MSE=zeros(20,50);
for n=1:50
    x=dlmread(['surr_' num2str(n) '-mse-out']);
    MSE(:,n)=x(:,2);
end

plot(x(:,1),mean(MSE,2),'r-o')