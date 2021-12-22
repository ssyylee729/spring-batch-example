package spring.batch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.retry.support.RetryTemplateBuilder;

@Slf4j
public class PersonValidationRetryProcessor implements ItemProcessor<Person, Person> {

    private final RetryTemplate retryTemplate;

    public PersonValidationRetryProcessor() {
        this.retryTemplate = new RetryTemplateBuilder()
                .maxAttempts(3)
                .retryOn(NotFoundNameException.class)
                .withListener(new SavePersonRetryListener())
                .build();

    }

    @Override
    public Person process(Person item) throws Exception {
        return this.retryTemplate.execute(context -> {
            // RetryCallback : retryTemplate의 처음 시작 지점

            if (item.isNotEmptyName()) {
                return item;
            }

            throw new NotFoundNameException();
        }, context -> {
            // RecoveryCallback : 위에서 maxAttempts 수만큼 RetryCallback 실행한 이후의 행동
            return item.unknownName();
        });
    }

    public static class SavePersonRetryListener implements RetryListener {
        @Override
        public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
//            log.info("retry context, {}");
             if(context.getLastThrowable()!= null && context.getLastThrowable().getMessage().equals("Stop")) return false;
            return true;
            //open은 return true여야 RetryTemplate이 동작함.
        }

        @Override
        public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
            log.info("close");
        }

        @Override
        public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
            log.info("error context,{}", context);
            log.info("onError");
        }
    }

}